from confluent_kafka import Consumer
import json
import grpc
import notifier_ue_pb2
import notifier_ue_pb2_grpc
from email.message import EmailMessage
import ssl
import smtplib
import mysql.connector
import os


def commit_completed(err):
    if err:
        print(str(err))
    else:
        print("Notification fetched and stored in DB in order to be sent!")


def fetch_email(userId):
    #communication with user management in order to get user email
    with grpc.insecure_channel('user_management:50051') as channel:
        try:
            stub = notifier_ue_pb2_grpc.NotifierUeStub(channel)
            response = stub.RequestEmail(notifier_ue_pb2.Request(user_id=userId))
            print(response.email)
            email = response.email
            return email
        except grpc.RpcError as error:
            print("gRPC error!\n" + str(error))
            return "null"


def update_event_sent(id):
    #connection with DB and update the entry of the notification sent
    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port= os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
        try:
            mycursor = mydb.cursor()
            mycursor.execute("UPDATE events SET sent=TRUE WHERE id = %s", (str(id), ))
            mydb.commit()
        except mysql.connector.Error as e:
            print("Exception raised!\n" + str(e))


def send_email(email):
    #send notification by email
    email_sender = "noreplydsbd@gmail.com"
    email_password = os.environ.get('APP_PASSWORD')
    email_receiver = email
    subject = "Alert Notification!"
    body = " messaggio di prova"   #TODO: da modificare con l'elenco delle rules violate
    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    em.set_content(body)
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        try:
            smtp.login(email_sender, email_password)
            smtp.sendmail(email_sender, email_receiver, em.as_string())
            return True
        except smtplib.SMTPException as exception:
            mydb.rollback()
            print("SMTP protocol error!\n" + str(exception))
            return False


def find_event_not_sent():
    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port= os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
        try:
            mycursor = mydb.cursor()
            mycursor.execute("SELECT * FROM events WHERE sent=FALSE)")
            results = mycursor.fetchall()
            for x in results:
                email = fetch_email(x[1])
                if email == "null":
                    return False
                result = send_email(email)
                if result != True:
                    return False
                update_event_sent(x[0])
            return True

        except mysql.connector.Error as err:
            mydb.rollback()
            print("Exception raised!\n" + str(err))
            raise SystemExit #to terminate without Kafka commit


if __name__ == "__main__":

    c = Consumer({'bootstrap.servers': 'kafka-1:29092',
                  'group.id': 'group1',
                  'enable.auto.commit': 'false',
                  'auto.offset.reset': 'latest',
                  'on_commit': commit_completed
                  })
    c.subscribe(['event_to_be_notified'])
    try:
        while True:
            result = find_event_not_sent()
            if result == False:
                continue
            msg = c.poll(timeout=5.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:

                # Check for Kafka message
                record_key = msg.key()
                print(record_key)
                record_value = msg.value()
                print(record_value)
                data = json.loads(record_value)
                userId = data['user_id']
                location = data['location_id']
                violated_rules = data['violated_rules']

                #connection with DB and store event to be notified
                with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port= os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
                    try:
                        mycursor = mydb.cursor()
                        mycursor.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER NOT NULL, location_id INTEGER NOT NULL, rules VARCHAR(100000) NOT NULL, time_stamp TIMESTAMP NOT NULL, sent BOOLEAN NOT NULL)")
                        mycursor.execute("INSERT INTO events VALUES(%s, %s, %s, %s, %s)", (str(userId), str(location), str(violated_rules), "CURRENT_TIMESTAMP()", "FALSE"))
                        mydb.commit()  #to make changes effective
                        last_id = mycursor.lastrowid  #in order to get the ID of the latest row added
                    except mysql.connector.Error as err:
                        mydb.rollback()
                        print("Exception raised!\n" + str(err))
                        raise SystemExit #to terminate without Kafka commit

                #make commit
                try:
                    c.commit(asynchronous=True)
                    print("Commit done!")
                except Exception as e:
                    print("Error in commit!\n" + str(e))
                    raise SystemExit

    except (KeyboardInterrupt, SystemExit): #to terminate correctly with either CTRL+C or docker stop
        pass
    finally:
        # Leave group and commit final offsets
        c.close()

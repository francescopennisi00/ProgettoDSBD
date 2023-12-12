from confluent_kafka import Consumer
import json
import grpc
import notifier_um_pb2
import notifier_um_pb2_grpc
from email.message import EmailMessage
import ssl
import smtplib
import mysql.connector
import os


def commit_completed(er):
    if er:
        print(str(er))
    else:
        print("Notification fetched and stored in DB in order to be sent!")


# communication with user management in order to get user email
def fetch_email(userid):
    with grpc.insecure_channel('user_management:50051') as channel:
        try:
            stub = notifier_um_pb2_grpc.NotifierUmStub(channel)
            response = stub.RequestEmail(notifier_um_pb2.Request(user_id=userid))
            print("Fetched email: " + response.email)
            return response.email
        except grpc.RpcError as error:
            print("gRPC error!\n" + str(error))
            return "null"


# connection with DB and update the entry of the notification sent
def update_event_sent(event_id):
    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as db:
        try:
            cursor = db.cursor()
            cursor.execute("UPDATE events SET sent=TRUE WHERE id = %s", (str(event_id),))
            db.commit()
            return True
        except mysql.connector.Error as error:
            print("Exception raised!\n" + str(error))
            db.rollback()
            return False


# send notification by email
def send_email(email):
    email_sender = os.environ.get('EMAIL')
    email_password = os.environ.get('APP_PASSWORD')
    email_receiver = email
    subject = "Alert Notification!"
    body = " messaggio di prova"  # TODO: da modificare con l'elenco delle rules violate
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
            print("SMTP protocol error!\n" + str(exception))
            return False


# find events to send, send them by email and update events in DB
def find_event_not_sent():
    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as db:
        try:
            cursor = db.cursor()
            cursor.execute("SELECT * FROM events WHERE sent=FALSE")
            results = cursor.fetchall()
            for x in results:
                email = fetch_email(x[1])
                if email == "null":
                    return False
                res = send_email(email)
                if res != True:
                    return False
                # we give 5 attempts to try to update the DB in order
                # to avoid resending the email as much as possible
                for t in range(5):
                    if update_event_sent(x[0]) == True:
                        break
                else: raise SystemExit


        except mysql.connector.Error as error:
            print("Exception raised!\n" + str(error))
            raise SystemExit


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

                # connection with DB and store event to be notified
                with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
                    try:
                        mycursor = mydb.cursor()
                        mycursor.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER NOT NULL, location_id INTEGER NOT NULL, rules VARCHAR(100000) NOT NULL, time_stamp TIMESTAMP NOT NULL, sent BOOLEAN NOT NULL)")
                        mycursor.execute("INSERT INTO events VALUES(%s, %s, %s, %s, %s)", (str(userId), str(location), str(violated_rules), "CURRENT_TIMESTAMP()", "FALSE"))
                        mydb.commit()  # to make changes effective
                    except mysql.connector.Error as err:
                        mydb.rollback()
                        print("Exception raised!\n" + str(err))
                        raise SystemExit  # to terminate without Kafka commit

                # make commit
                try:
                    c.commit(asynchronous=True)
                    print("Commit done!")
                except Exception as e:
                    print("Error in commit!\n" + str(e))
                    raise SystemExit

    except (KeyboardInterrupt, SystemExit):  # to terminate correctly with either CTRL+C or docker stop
        pass
    finally:
        # Leave group and commit final offsets
        c.close()

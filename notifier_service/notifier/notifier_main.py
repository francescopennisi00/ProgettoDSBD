import confluent_kafka
import json
import grpc
import notifier_um_pb2
import notifier_um_pb2_grpc
from email.message import EmailMessage
import ssl
import smtplib
import mysql.connector
import os
import time
import sys


def commit_completed(er):
    if er:
        print(str(er))
    else:
        print("Notification fetched and stored in DB in order to be sent!")


# communication with user management in order to get user email
def fetch_email(userid):
    try:
        with grpc.insecure_channel('um_service:50051') as channel:
            stub = notifier_um_pb2_grpc.NotifierUmStub(channel)
            response = stub.RequestEmail(notifier_um_pb2.Request(user_id=userid))
            print("Fetched email: " + response.email)
            email_to_return = response.email
    except grpc.RpcError as error:
        sys.stderr.write("gRPC error!\n" + str(error))
        email_to_return = "null"
    return email_to_return


# connection with DB and update the entry of the notification sent
def update_event_sent(event_id):
    try:
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as db:
            cursor = db.cursor()
            cursor.execute("UPDATE events SET sent=TRUE WHERE id = %s", (str(event_id),))
            db.commit()
            boolean_to_return = True
    except mysql.connector.Error as error:
        sys.stderr.write("Exception raised!\n" + str(error))
        boolean_to_return = False
        try:
            db.rollback()
        except Exception as exc:
            sys.stderr.write(f"Exception raised in rollback: {exc}")
    return boolean_to_return


# send notification by email
def send_email(email):
    email_sender = os.environ.get('EMAIL')
    email_password = os.environ.get('APP_PASSWORD')
    email_receiver = email
    subject = "Alert Notification!"
    body = " messaggio di prova"  # TODO: da modificare con l'elenco delle rules violate (come parametro)
    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    em.set_content(body)
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(email_sender, email_password)
            smtp.sendmail(email_sender, email_receiver, em.as_string())
            boolean_to_return = True
    except smtplib.SMTPException as exception:
        sys.stderr.write("SMTP protocol error!\n" + str(exception))
        boolean_to_return = False
    return boolean_to_return


# find events to send, send them by email and update events in DB
def find_event_not_sent():
    try:
        db = mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE'))
        cursor = db.cursor()
        cursor.execute("SELECT * FROM events WHERE sent=FALSE")
        results = cursor.fetchall()
        for x in results:
            email = fetch_email(x[1])
            if email == "null":
                db.close()
                return False
            res = send_email(email)  #TODO: other params required
            if res != True:
                db.close()
                return False
            # we give 5 attempts to try to update the DB in order
            # to avoid resending the email as much as possible
            for t in range(5):
                if update_event_sent(x[0]) == True:
                    break
                time.sleep(1)
            else:
                raise SystemExit

    except mysql.connector.Error as error:
        sys.stderr.write("Exception raised!\n" + str(error))
        raise SystemExit


if __name__ == "__main__":

    # setting env variables for secrets
    secret_password_path = os.environ.get('PASSWORD')
    with open(secret_password_path, 'r') as file:
        secret_password_value = file.read()
    os.environ['PASSWORD'] = secret_password_value
    secret_app_password_path = os.environ.get('APP_PASSWORD')
    with open(secret_app_password_path, 'r') as file:
        secret_app_password_value = file.read()
    os.environ['APP_PASSWORD'] = secret_app_password_value
    secret_email_path = os.environ.get('EMAIL')
    with open(secret_email_path, 'r') as file:
        secret_email_value = file.read()
    os.environ['EMAIL'] = secret_email_value

    # start Kafka subscription
    c = confluent_kafka.Consumer({'bootstrap.servers': 'kafka:29092', 'group.id': 'group1', 'enable.auto.commit': 'false', 'auto.offset.reset': 'latest', 'on_commit': commit_completed})
    try:
        c.subscribe(['event_to_be_notified'])
    except confluent_kafka.KafkaException as ke:
        sys.stderr.write("Kafka exception raised!\n" + str(ke))
        c.close()
        sys.exit("Terminate after Exception raised in Kafka topic subscribe")

    try:
        while True:
            # Creating table if not exits
            try:
                with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
                    mycursor = mydb.cursor()
                    # TODO: e' necessaria la location id o il nome della localita'? Anche il notifier
                    # TODO: ha la tabella locations o le informazioni sulla localita' gli vengono
                    # TODO: fornite? Rispondere dopo aver completato il worker!
                    mycursor.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER NOT NULL, location_id INTEGER NOT NULL, rules JSON NOT NULL, time_stamp TIMESTAMP NOT NULL, sent BOOLEAN NOT NULL)")
                    mydb.commit()  # to make changes effective
            except mysql.connector.Error as err:
                sys.stderr.write("Exception raised!\n" + str(err))
                try:
                    mydb.rollback()
                except Exception as excep:
                    sys.stderr.write(f"Exception raised in rollback: {excep}")
                raise SystemExit

            # Looking for entries that have sent == False
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
                if msg.error().code() == confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise SystemExit
            else:
                # Check for Kafka message
                record_key = msg.key()
                print(record_key)
                record_value = msg.value()
                print(record_value)
                data = json.loads(record_value)
                # TODO: to reiviewed after completing the worker
                userId = data['user_id']
                location = data['location_id']
                violated_rules = data['violated_rules']

                # connection with DB and store event to be notified
                try:
                    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
                        mycursor = mydb.cursor()
                        mycursor.execute("INSERT INTO events VALUES(%s, %s, %s, %s, %s)", (str(userId), str(location), str(violated_rules), "CURRENT_TIMESTAMP()", "FALSE"))
                        mydb.commit()  # to make changes effective
                except mysql.connector.Error as err:
                    sys.stderr.write("Exception raised!\n" + str(err))
                    try:
                        mydb.rollback()
                    except Exception as exe:
                        sys.stderr.write(f"Exception raised in rollback: {exe}")
                    raise SystemExit  # to terminate without Kafka commit

                # make commit
                try:
                    c.commit(asynchronous=True)
                    print("Commit done!")
                except Exception as e:
                    sys.stderr.write("Error in commit!\n" + str(e))
                    raise SystemExit

    except (KeyboardInterrupt, SystemExit):  # to terminate correctly with either CTRL+C or docker stop
        pass
    finally:
        # Leave group and commit final offsets
        c.close()

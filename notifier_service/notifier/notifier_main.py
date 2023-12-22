import confluent_kafka
# from confluent_kafka.admin import AdminClient
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


def commit_completed(er, partitions):
    if er:
        print(str(er))
    else:
        print("Commit done!\n")
        print("Committed partition offsets: " + str(partitions) + "\n")
        print("Notification fetched and stored in DB in order to be sent!\n")


# communication with user management in order to get user email
def fetch_email(userid):
    try:
        with grpc.insecure_channel('um_service:50051') as channel:
            stub = notifier_um_pb2_grpc.NotifierUmStub(channel)
            response = stub.RequestEmail(notifier_um_pb2.Request(user_id=userid))
            print("Fetched email: " + response.email + "\n")
            email_to_return = response.email
    except grpc.RpcError as error:
        sys.stderr.write("gRPC error! -> " + str(error) + "\n")
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
        sys.stderr.write("Exception raised! -> " + str(error) + "\n")
        boolean_to_return = False
        try:
            db.rollback()
        except Exception as exc:
            sys.stderr.write(f"Exception raised in rollback: {exc}\n")
    return boolean_to_return


def insert_rule_in_mail_text(rule, value, name_loc, country, state):
    if rule == "max_temp" or rule == "min_temp":
        return f"The temperature in {name_loc} ({country}, {state}) is {str(value)} °C!\n"
    elif rule == "max_humidity" or rule == "min_humidity":
        return f"The humidity in {name_loc} ({country}, {state}) is {str(value)} %!\n"
    elif rule == "max_pressure" or rule == "min_pressure":
        return f"The pressure in {name_loc} ({country}, {state}) is {str(value)} hPa!\n"
    elif rule == "clouds_max" or rule == "clouds_min":
        return f"The the percentage of sky covered by clouds in {name_loc} ({country}, {state}) is {str(value)} %\n"
    elif rule == "max_wind_speed" or rule == "min_wind_speed":
        return f"The wind speed in {name_loc} ({country}, {state}) is {str(value)} m/s!\n"
    elif rule == "wind_direction":
        return f"The wind direction in {name_loc} ({country}, {state}) is {value}!\n"
    elif rule == "rain":
        return f"Warning! In {name_loc} ({country}, {state}) is raining! Arm yourself with an umbrella!\n"
    elif rule == "snow":
        return f"Warning! In {name_loc} ({country}, {state}) is snowing! Be careful and enjoy the snow!\n"


# send notification by email
def send_email(email, violated_rules, name_location, country, state):
    email_sender = os.environ.get('EMAIL')
    email_password = os.environ.get('APP_PASSWORD')
    email_receiver = email
    subject = "Weather Alert Notification! "
    body = "Warning! Some weather parameters that you specified have been violated!\n\n"
    rules_list = violated_rules.get("violated_rules")  # extracting list of key-value pairs of violated_rules
    for element in rules_list:
        if element:
            rule = next(iter(element))
            body += insert_rule_in_mail_text(rule, element[rule], name_location, country, state)
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
        sys.stderr.write("SMTP protocol error! -> " + str(exception) + "\n")
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
                cursor.close()
                db.close()
                return False
            loc_name = x[2]
            loc_country = x[3]
            loc_state = x[4]
            violated_rules = json.loads(x[5])
            res = send_email(email, violated_rules, loc_name, loc_country, loc_state)
            if res != True:
                cursor.close()
                db.close()
                return "error_in_send_email"
            # we give 5 attempts to try to update the DB in order
            # to avoid resending the email as much as possible
            for t in range(5):
                if update_event_sent(x[0]) == True:
                    break
                time.sleep(1)
            else:
                raise SystemExit

    except mysql.connector.Error as error:
        sys.stderr.write("Exception raised! -> " + str(error) +"\n")
        raise SystemExit


if __name__ == "__main__":

    print("Start notifier main")

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

    print("ENV variables initialization done")

    # start Kafka subscription (if "event_to_be_notified" exists, else exit)
    c = confluent_kafka.Consumer({'bootstrap.servers':'kafka:9092', 'group.id':'group1', 'enable.auto.commit':'false', 'auto.offset.reset':'latest', 'on_commit':commit_completed})
    try:
        broker = 'kafka:9092'
        topic = 'event_to_be_notified'
        #admin_conf = {'bootstrap.servers': broker}
        #kadmin = AdminClient(admin_conf)
        #list_topics_metadata = kadmin.list_topics()
        #topics = list_topics_metadata.topics  # Returns a dict()
        #print(f"LIST_TOPICS: {list_topics_metadata}")
        #print(f"TOPICS: {topics}")
        #topic_names = set(topics.keys())
        #print(f"TOPIC_NAMES: {topic_names}")
        #found = False
        #for name in topic_names:
        #    if name == 'event_to_be_notified':
        #        found = True
        #        print(f"Topic {name} found: subscribe!")
        c.subscribe(['event_to_be_notified'])
        #if found == False:
        #    sys.exit("Terminate because Kafka topic to subscribe has been not found\n")
    except confluent_kafka.KafkaException as ke:
        sys.stderr.write("Kafka exception raised! -> " + str(ke) + "\n")
        c.close()
        sys.exit("Terminate after Exception raised in Kafka topic subscribe\n")
    except Exception as ke:
        sys.stderr.write("Kafka exception raised! -> " + str(ke) + "\n")
        c.close()
        sys.exit("Terminate after GENERAL Exception raised in Kafka subscription\n")

    print("Starting while true\n")

    try:
        while True:

            print("New iteration!\n")

            # Creating table if not exits
            try:
                with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
                    mycursor = mydb.cursor()
                    mycursor.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER NOT NULL, location_name VARCHAR(70) NOT NULL, location_country VARCHAR(10) NOT NULL, location_state VARCHAR(30) NOT NULL, rules JSON NOT NULL, time_stamp TIMESTAMP NOT NULL, sent BOOLEAN NOT NULL)")
                    mydb.commit()  # to make changes effective
            except mysql.connector.Error as err:
                sys.stderr.write("Exception raised! -> " + str(err) +"\n")
                try:
                    mydb.rollback()
                except Exception as excep:
                    sys.stderr.write(f"Exception raised in rollback: {excep}\n")
                raise SystemExit

            # Looking for entries that have sent == False
            result = find_event_not_sent()
            if result == "error_in_send_email":
                continue

            msg = c.poll(timeout=5.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()\n")
                continue
            elif msg.error():
                print('error: {}\n'.format(msg.error()))
                if msg.error().code() == confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise SystemExit
            else:
                # Check for Kafka message
                record_key = msg.key()
                print("REDCORD KEY " + str(record_key))
                record_value = msg.value()
                print("RECORD VALUE " + str(record_value))
                data = json.loads(record_value)
                location_name = data.get("location")[0]
                location_country = data.get("location")[3]
                location_state = data.get("location")[4]
                del data["location"]
                user_id_set = set(data.keys())
                # connection with DB and store events to be notified
                try:
                    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
                        mycursor = mydb.cursor()
                        for user_id in user_id_set:
                            temp_dict = dict()
                            temp_dict["violated_rules"] = data.get(user_id)
                            violated_rules = json.dumps(temp_dict)
                            mycursor.execute("INSERT INTO events (user_id, location_name, location_country, location_state, rules, time_stamp, sent) VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, FALSE)", (str(user_id), location_name, location_country, location_state, violated_rules))
                        mydb.commit()  # to make changes effective after inserting ALL the violated_rules
                except mysql.connector.Error as err:
                    sys.stderr.write("Exception raised! -> " + str(err) + "\n")
                    try:
                        mydb.rollback()
                    except Exception as exe:
                        sys.stderr.write(f"Exception raised in rollback: {exe}\n")
                    raise SystemExit  # to terminate without Kafka commit

                # make commit
                try:
                    c.commit(asynchronous=True)
                except Exception as e:
                    sys.stderr.write("Error in commit! -> " + str(e) +"\n")
                    raise SystemExit

    except (KeyboardInterrupt, SystemExit):  # to terminate correctly with either CTRL+C or docker stop
        pass
    finally:
        # Leave group and commit final offsets
        c.close()

import confluent_kafka
import json
import grpc
import notifier_um_pb2
import notifier_um_pb2_grpc
from email.message import EmailMessage
import ssl
import smtplib
import socket
import mysql.connector
import os
import time
import sys
import threading
from flask import Flask
from prometheus_client import Counter, generate_latest, REGISTRY, Gauge, Histogram
from flask import Response
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# definition of the metrics to be exposed
NOTIFICATIONS = Counter('NOTIFIER_notifications_sent', 'Total number of notifications sent')
NOTIFICATIONS_ERROR = Counter('NOTIFIER_notifications_error', 'Total number of errors in sending email')
DELTA_TIME = Gauge('NOTIFIER_notification_latency_nanoseconds', 'Latency beetween instant in which worker publishes the message and instant in which notifier sends email')
QUERY_DURATIONS_HISTOGRAM = Histogram('NOTIFIER_query_durations_nanoseconds_DB', 'DB query durations in nanoseconds', buckets=[5000000, 10000000, 25000000, 50000000, 75000000, 100000000, 250000000, 500000000, 750000000, 1000000000, 2500000000,5000000000,7500000000,10000000000])
# buckets indicated because of measuring time in nanoseconds


def commit_completed(er, partitions):
    if er:
        logger.error(str(er))
    else:
        logger.info("Commit done!\n")
        logger.info("Committed partition offsets: " + str(partitions) + "\n")
        logger.info("Notification fetched and stored in DB in order to be sent!\n")


# communication with user management in order to get user email
def fetch_email(userid):
    try:
        with grpc.insecure_channel('um-service:50051') as channel:
            stub = notifier_um_pb2_grpc.NotifierUmStub(channel)
            response = stub.RequestEmail(notifier_um_pb2.Request(user_id=userid))
            logger.info("Fetched email: " + response.email + "\n")
            email_to_return = response.email
    except grpc.RpcError as error:
        logger.error("gRPC error! -> " + str(error) + "\n")
        email_to_return = "null"
    return email_to_return


# connection with DB and update the entry of the notification sent
def update_event_sent(event_id):
    try:
        DBstart_time = time.time_ns()
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as db:
            cursor = db.cursor()
            cursor.execute("UPDATE events SET sent=TRUE, time_stamp=CURRENT_TIMESTAMP WHERE id = %s", (str(event_id),))
            db.commit()
            DBend_time = time.time_ns()
            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
            boolean_to_return = True
    except mysql.connector.Error as error:
        logger.error("Exception raised! -> " + str(error) + "\n")
        boolean_to_return = False
        try:
            db.rollback()
        except Exception as exc:
            logger.error(f"Exception raised in rollback: {exc}\n")
    return boolean_to_return


def insert_rule_in_mail_text(rule, value, name_loc, country, state):
    if rule == "max_temp" or rule == "min_temp":
        return f"The temperature in {name_loc} ({country}, {state}) is {str(value)} °C!\n"
    elif rule == "max_humidity" or rule == "min_humidity":
        return f"The humidity in {name_loc} ({country}, {state}) is {str(value)} %!\n"
    elif rule == "max_pressure" or rule == "min_pressure":
        return f"The pressure in {name_loc} ({country}, {state}) is {str(value)} hPa!\n"
    elif rule == "max_cloud" or rule == "min_cloud":
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
        logger.error("SMTP protocol error! -> " + str(exception) + "\n")
        boolean_to_return = False
    return boolean_to_return


# find events to send, send them by email and update events in DB
def find_event_not_sent():
    try:
        DBstart_time = time.time_ns()
        db = mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE'))
        cursor = db.cursor()
        cursor.execute("SELECT * FROM events WHERE sent=FALSE AND notifier_id=%s", (notifier_id,))
        results = cursor.fetchall()
        DBend_time = time.time_ns()
        QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
        for x in results:
            email = fetch_email(x[1])
            if email == "null":
                cursor.close()
                db.close()
                return False
            if email == "not present anymore":
                DBstart_time = time.time_ns()
                cursor.execute("DELETE FROM events WHERE id=%s", (x[0], ))
                db.commit()
                DBend_time = time.time_ns()
                QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                continue
            loc_name = x[2]
            loc_country = x[3]
            loc_state = x[4]
            violated_rules = json.loads(x[5])
            res = send_email(email, violated_rules, loc_name, loc_country, loc_state)
            if res != True:
                cursor.close()
                db.close()
                NOTIFICATIONS_ERROR.inc()
                return "error_in_send_email"
            #  violated_rules contains also a key value pair with the timestamp of the kafka message publications
            #  This is important for exposure the notification latency metric
            end_time = time.time_ns()
            start_time = violated_rules.get("timestamp_worker")
            DELTA_TIME.set(end_time-start_time)
            NOTIFICATIONS.inc()
            # we give 5 attempts to try to update the DB in order
            # to avoid resending the email as much as possible
            for t in range(5):
                if update_event_sent(x[0]) == True:
                    break
                time.sleep(1)
            else:
                raise SystemExit

    except mysql.connector.Error as error:
        logger.error("Exception raised! -> " + str(error) +"\n")
        try:
            mydb.rollback()
        except Exception as exe:
            logger.error(f"Exception raised in rollback: {exe}\n")
        raise SystemExit


def create_app():
    app = Flask(__name__)

    @app.route('/metrics')
    def metrics():
        # Export all the metrics as text for Prometheus
        return Response(generate_latest(REGISTRY), mimetype='text/plain')

    return app


def serve_prometheus():
    port = 50055
    hostname = socket.gethostname()
    logger.info(f'Hostname: {hostname} -> server starting on port {str(port)}')
    app.run(host='0.0.0.0', port=port, threaded=True)


notifier_id = os.environ.get(("NOTIFIERID"))


# create Flask application
app = create_app()

if __name__ == "__main__":

    logger.info("Start notifier main")

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

    logger.info("ENV variables initialization done")

    logger.info("Starting Prometheus serving thread!\n")
    threadAPIGateway = threading.Thread(target=serve_prometheus)
    threadAPIGateway.daemon = True
    threadAPIGateway.start()

    # start Kafka subscription
    c = confluent_kafka.Consumer({'bootstrap.servers':'kafka-service:9092', 'group.id':'group1', 'enable.auto.commit':'false', 'auto.offset.reset':'latest', 'on_commit':commit_completed})
    try:
        broker = 'kafka:9092'
        topic = 'event_to_be_notified'
        c.subscribe(['event_to_be_notified'])
    except confluent_kafka.KafkaException as ke:
        logger.error("Kafka exception raised! -> " + str(ke) + "\n")
        c.close()
        sys.exit("Terminate after Exception raised in Kafka topic subscribe\n")
    except Exception as ke:
        logger.error("Kafka exception raised! -> " + str(ke) + "\n")
        c.close()
        sys.exit("Terminate after GENERAL Exception raised in Kafka subscription\n")

    logger.info("Starting while true\n")

    try:
        while True:

            logger.info("New iteration!\n")

            # Creating table if not exits
            try:
                DBstart_time = time.time_ns()
                with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
                    mycursor = mydb.cursor()
                    mycursor.execute("CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER NOT NULL, location_name VARCHAR(70) NOT NULL, location_country VARCHAR(10) NOT NULL, location_state VARCHAR(30) NOT NULL, rules JSON NOT NULL, time_stamp TIMESTAMP NOT NULL, sent BOOLEAN NOT NULL, notifier_id VARCHAR(60) NOT NULL, INDEX notifier_ind (notifier_id))")
                    mydb.commit()  # to make changes effective
                    DBend_time = time.time_ns()
                    QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
            except mysql.connector.Error as err:
                logger.error("Exception raised! -> " + str(err) +"\n")
                try:
                    mydb.rollback()
                except Exception as excep:
                    logger.error(f"Exception raised in rollback: {excep}\n")
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
                logger.info("Waiting for message or event/error in poll()\n")
                continue
            elif msg.error():
                logger.info('error: {}\n'.format(msg.error()))
                if msg.error().code() == confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise SystemExit
            else:
                # Check for Kafka message
                record_key = msg.key()
                logger.info("RECORD KEY " + str(record_key))
                record_value = msg.value()
                logger.info("RECORD VALUE " + str(record_value))
                data = json.loads(record_value)
                location_name = data.get("location")[0]
                location_country = data.get("location")[3]
                location_state = data.get("location")[4]
                del data["location"]
                worker_timestamp = data.get("timestamp")  # Kafka msg has a key-value pair with worker publish timestamp: important for latency of notification metric
                del data["timestamp"]  # now all the keys of data are user_ids
                user_id_set = set(data.keys())
                # connection with DB and store events to be notified
                try:
                    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
                        mycursor = mydb.cursor()
                        for user_id in user_id_set:
                            temp_dict = dict()
                            temp_dict["violated_rules"] = data.get(user_id)
                            temp_dict["timestamp_worker"] = worker_timestamp
                            violated_rules = json.dumps(temp_dict)
                            DBstart_time = time.time_ns()
                            mycursor.execute("INSERT INTO events (user_id, location_name, location_country, location_state, rules, time_stamp, sent, notifier_id) VALUES(%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, FALSE, %s)", (str(user_id), location_name, location_country, location_state, violated_rules, notifier_id))
                            DBend_time = time.time_ns()
                            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                        mydb.commit()  # to make changes effective after inserting ALL the violated_rules
                except mysql.connector.Error as err:
                    logger.error("Exception raised! -> " + str(err) + "\n")
                    try:
                        mydb.rollback()
                    except Exception as exe:
                        logger.error(f"Exception raised in rollback: {exe}\n")
                    raise SystemExit  # to terminate without Kafka commit

                # make commit
                try:
                    c.commit(asynchronous=True)
                except Exception as e:
                    logger.error("Error in commit! -> " + str(e) + "\n")
                    raise SystemExit

    except (KeyboardInterrupt, SystemExit):  # to terminate correctly with either CTRL+C or docker stop
        pass
    finally:
        # Leave group and commit final offsets
        c.close()

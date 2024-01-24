import time
import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic
import json
import mysql.connector
import os
import sys
import requests
import socket
import threading
from flask import Flask
from prometheus_client import Counter, generate_latest, REGISTRY, Gauge, Histogram
from flask import Response
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# definition of the metrics to be exposed
ERROR_REQUEST_OPEN_WEATHER = Counter('WORKER_error_request_OpenWeather', 'Total number of requests sent to OpenWeather that failed')
REQUEST_OPEN_WEATHER = Counter('WORKER_requests_to_OpenWeather', 'Total number of API calls to OpenWeather')
DELTA_TIME = Gauge('WORKER_response_time_OpenWeather', 'Difference between instant when worker sends request to OpenWeather and instant when it receives the response')
KAFKA_MESSAGE = Counter('WORKER_kafka_message_number', 'Total number of kafka messages produced by worker-service')
KAFKA_MESSAGE_DELIVERED = Counter('WORKER_kafka_message_delivered_number', 'Total number of kafka messages produced by worker-service that have been delivered correctly')
QUERY_DURATIONS_HISTOGRAM = Histogram('WORKER_query_durations_nanoseconds_DB', 'DB query durations in nanoseconds', buckets=[5000000, 10000000, 25000000, 50000000, 75000000, 100000000, 250000000, 500000000, 750000000, 1000000000, 2500000000,5000000000,7500000000,10000000000])
# buckets indicated because of measuring time in nanoseconds


def commit_completed(er, partitions):
    if er:
        logger.error(str(er))
    else:
        logger.info("Commit done!\n")
        logger.info("Committed partition offsets: " + str(partitions) + "\n")
        logger.info("Rules fetched and stored in DB in order to save current work!\n")


def make_query(query):
    start_time = time.time_ns()
    try:
        REQUEST_OPEN_WEATHER.inc()
        resp = requests.get(url=query)
        resp.raise_for_status()
        response = resp.json()
        end_time = time.time_ns()
        DELTA_TIME.set(end_time-start_time)
        if response.get('cod') != 200:
            ERROR_REQUEST_OPEN_WEATHER.inc()
            raise Exception('Query failed: ' + response.get('message'))
        logger.info(json.dumps(response) + "\n")
        return response
    except requests.JSONDecodeError as er:
        ERROR_REQUEST_OPEN_WEATHER.inc()
        end_time = time.time_ns()
        DELTA_TIME.set(end_time-start_time)
        logger.error(f'JSON Decode error: {er}\n')
        raise SystemExit
    except requests.HTTPError as er:
        ERROR_REQUEST_OPEN_WEATHER.inc()
        end_time = time.time_ns()
        DELTA_TIME.set(end_time-start_time)
        logger.error(f'HTTP Error: {er}\n')
        raise SystemExit
    except requests.exceptions.RequestException as er:
        ERROR_REQUEST_OPEN_WEATHER.inc()
        end_time = time.time_ns()
        DELTA_TIME.set(end_time-start_time)
        logger.error(f'Request failed: {er}\n')
        raise SystemExit
    except Exception as er:
        logger.error(f'Error: {er}\n')
        raise SystemExit


# compare values obtained from OpenWeather API call with those that have been placed into the DB
# for recoverability from faults that occur before to possibly publish violated rules
# returns the violated rules to be sent in the form of a dictionary that contains many other
# dictionary with key = user_id and value = the list of (violated rule-current value) pairs
# there is another key-value pair in the outer dictionary with key = "location" and value = array
# that contains information about the location in common for all the entries to be entered into the DB
def check_rules(db_cursor, api_response):
    DBstart_time = time.time_ns()
    db_cursor.execute("SELECT rules FROM current_work WHERE worker_id = %s", (str(worker_id),))
    rules_list = db_cursor.fetchall()
    DBend_time = time.time_ns()
    QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
    event_dict = dict()
    for rules in rules_list:
        user_violated_rules_list = list()
        rules_json = json.loads(rules[0])
        keys_set_target = set(rules_json.keys())
        for key in keys_set_target:
            temp_dict = dict()
            if "max" in key and rules_json.get(key) != "null":
                if api_response.get(key) > rules_json.get(key):
                    temp_dict[key] = api_response.get(key)
            elif "min" in key and rules_json.get(key) != "null":
                if api_response.get(key) < rules_json.get(key):
                    temp_dict[key] = api_response.get(key)
            elif key == "rain" and rules_json.get(key) != "null" and api_response.get("rain") == True:
                temp_dict[key] = api_response.get(key)
            elif key == "snow" and rules_json.get(key) != "null" and api_response.get("snow") == True:
                temp_dict[key] = api_response.get(key)
            elif key == "wind_direction" and rules_json.get(key) != "null" and rules_json.get(key) == api_response.get(key):
                temp_dict[key] = api_response.get(key)
            user_violated_rules_list.append(temp_dict)
        event_dict[rules_json.get("user_id")] = user_violated_rules_list
    json_location = rules_list[0][0]  # all entries in rules_list have the same location
    dict_location = json.loads(json_location)
    event_dict['location'] = dict_location.get('location')
    return json.dumps(event_dict)


# function to formatting data returned by OpenWeather API according to our business logic
def format_data(data):
    output_json_dict = dict()
    output_json_dict['max_temp'] = data["main"]["temp"]
    output_json_dict['min_temp'] = data["main"]["temp"]
    output_json_dict['max_humidity'] = data["main"]["humidity"]
    output_json_dict['min_humidity'] = data["main"]["humidity"]
    output_json_dict['max_pressure'] = data["main"]["pressure"]
    output_json_dict['min_pressure'] = data["main"]["pressure"]
    output_json_dict["max_wind_speed"] = data["wind"]["speed"]
    output_json_dict["min_wind_speed"] = data["wind"]["speed"]
    output_json_dict["max_cloud"] = data["clouds"]["all"]
    output_json_dict["min_cloud"] = data["clouds"]["all"]
    direction_list = ["N", "NE", "E", " SE", "S", "SO", "O", "NO"]
    j = 0
    for i in range(0, 316, 45):
        if (i - 22.5) <= data["wind"]["deg"] <= (i + 22.5):
            output_json_dict["wind_direction"] = direction_list[j]
            break
        j = j + 1
    if data["weather"][0]["main"] == "Rain":
        output_json_dict["rain"] = True
    else:
        output_json_dict["rain"] = False
    if data["weather"][0]["main"] == "Snow":
        output_json_dict["snow"] = True
    else:
        output_json_dict["snow"] = False
    return output_json_dict


# function for recovering unchecked rules when worker goes down before publishing notification event
def find_current_work():
    try:
        DBstart_time = time.time_ns()
        with (mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                      user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                      database=os.environ.get('DATABASE')) as db_conn):
            # without a buffered cursor, the results are "lazily" loaded, meaning that "fetchone"
            # actually only fetches one row from the full result set of the query.
            # When you will use the same cursor again, it will complain that you still have
            # n-1 results (where n is the result set amount) waiting to be fetched.
            # However, when you use a buffered cursor the connector fetches ALL rows behind the scenes,
            # and you just take one from the connector so the mysql db won't complain.
            # buffered=True is needed because we next will use db_cursor as first parameter of check_rules
            db_cursor = db_conn.cursor(buffered=True)
            db_cursor.execute("SELECT rules FROM current_work WHERE worker_id = %s", (str(worker_id),))
            result = db_cursor.fetchone()
            DBend_time = time.time_ns()
            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
            if result:
                dict_row = json.loads(result[0])
                # all entries in current_works are related to the same location
                location_info = dict_row.get('location')
                # make OpenWeather API call
                apikey = os.environ.get('APIKEY')
                rest_call = f"https://api.openweathermap.org/data/2.5/weather?lat={location_info[1]}&lon={location_info[2]}&units=metric&appid={apikey}"
                data = make_query(rest_call)
                formatted_data = format_data(data)
                events_to_be_sent = check_rules(db_cursor, formatted_data)
            else:
                events_to_be_sent = "{}"
    except mysql.connector.Error as error:
        logger.error("Exception raised! -> " + str(error) + "\n")
        try:
            db_conn.rollback()
        except Exception as ex:
            logger.error(f"Exception raised in rollback: {ex}\n")
        return False
    return events_to_be_sent


# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        logger.error('%% Message failed delivery: %s\n' % err)
        raise SystemExit("Exiting after error in delivery message to Kafka broker\n")
    else:
        KAFKA_MESSAGE_DELIVERED.inc()
        logger.info('%% Message delivered to %s, partition[%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))
        try:
            DBstart_time = time.time_ns()
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as mydb:
                mycursor = mydb.cursor()
                mycursor.execute("DELETE FROM current_work WHERE worker_id = %s", (str(worker_id),))
                mydb.commit()  # to make changes effective
                DBend_time = time.time_ns()
                QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
        except mysql.connector.Error as err:
            logger.error("Exception raised!\n" + str(err))
            try:
                mydb.rollback()
            except Exception as exe:
                logger.error(f"Exception raised in rollback: {exe}\n")
            raise SystemExit


def produce_kafka_message(topic_name, kafka_producer, message):
    # Publish on the specific topic
    try:
        kafka_producer.produce(topic_name, value=message, callback=delivery_callback)
        KAFKA_MESSAGE.inc()
    except BufferError:
        logger.error(
            '%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(kafka_producer))
        return False
    # Wait until the message have been delivered
    logger.error("Waiting for message to be delivered\n")
    kafka_producer.flush()
    return True


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


worker_id = os.environ.get("WORKERID")

# create Flask application
app = create_app()


if __name__ == "__main__":

    # setting env variables for secrets
    secret_password_path = os.environ.get('PASSWORD')
    with open(secret_password_path, 'r') as file:
        secret_password_value = file.read()
    os.environ['PASSWORD'] = secret_password_value
    secret_apikey_path = os.environ.get('APIKEY')
    with open(secret_apikey_path, 'r') as file:
        secret_apikey_value = file.read()
    os.environ['APIKEY'] = secret_apikey_value

    logger.info("ENV variables initialization done")

    logger.info("Starting Prometheus serving thread!\n")
    threadAPIGateway = threading.Thread(target=serve_prometheus)
    threadAPIGateway.daemon = True
    threadAPIGateway.start()

    # create table current_work if not exists.
    # This table will contain many entries but all relating to the same message from the WMS
    # and therefore all with the same location
    # in particular, each entry includes a rules field that contains many key-value pairs in a JSON
    # in the form of { "user_id": value, "location": [name,lat,long,country,state],
    # "rule_name" : actual value, "other_rule_name" : actual value }
    # in the JSON there are all the rules in which the user is interested plus those in which
    # the user is not interested but for which at least one other user is interested in.
    # In this second case, the actual value of the rule is "null"
    try:
        DBstart_time = time.time_ns()
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()
            # we need worker_id in order to insert and remove only current work of worker replica
            # and don't affect current work of other replicas
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS current_work (id INTEGER PRIMARY KEY AUTO_INCREMENT, rules JSON NOT NULL, time_stamp TIMESTAMP NOT NULL, worker_id VARCHAR(60) NOT NULL, INDEX worker_ind (worker_id))")
            mydb.commit()  # to make changes effective
            DBend_time = time.time_ns()
            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
    except mysql.connector.Error as err:
        logger.error("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as exe:
            logger.error(f"Exception raised in rollback: {exe}\n")
        sys.exit("Exiting...\n")

    current_work = find_current_work()
    if current_work == False:
        sys.exit("Exiting after error in fetching rules to send\n")

    # Kafka admin and producer initialization in order to publish in topic "event_to_be_notified"
    broker = 'kafka-service:9092'
    topic = 'event_to_be_notified'
    producer_conf = {'bootstrap.servers': broker, 'acks': 1}
    admin_conf = {'bootstrap.servers': broker}
    kadmin = AdminClient(admin_conf)

    # Create topic "event_to_be_notified" if not exists
    list_topics_metadata = kadmin.list_topics()
    topics = list_topics_metadata.topics  # Returns a dict()
    logger.info(f"LIST_TOPICS: {list_topics_metadata}")
    logger.info(f"TOPICS: {topics}")
    topic_names = set(topics.keys())
    logger.info(f"TOPIC_NAMES: {topic_names}")
    found = False
    for name in topic_names:
        if name == topic:
            found = True
    if found == False:
        new_topic = NewTopic(topic, 1, 1)  # Number-of-partitions = 1, Number-of-replicas = 1
        kadmin.create_topics([new_topic,])

    # Create Producer instance
    producer_kafka = confluent_kafka.Producer(**producer_conf)

    # check if current work is pending and if it is true publish the rules to Kafka
    if current_work != '{}':  # JSON representation of an empty dictionary.
        current_work_dict = json.loads(current_work)
        current_work_dict['timestamp'] = time.time_ns()
        current_work = json.dumps(current_work_dict)
        while produce_kafka_message(topic, producer_kafka, current_work) == False:
            pass
    else:
        logger.info("There is no backlog of work\n")

    # start Kafka subscription
    consumer_kafka = confluent_kafka.Consumer(
        {'bootstrap.servers': 'kafka-service:9092', 'group.id': 'group2', 'enable.auto.commit': 'false',
         'auto.offset.reset': 'latest', 'on_commit': commit_completed})
    try:
        consumer_kafka.subscribe(['event_update'])  # the worker_service is also a Consumer related to the WMS Producer
    except confluent_kafka.KafkaException as ke:
        logger.error("Kafka exception raised! -> " + str(ke) + "\n")
        consumer_kafka.close()
        sys.exit("Terminate after Exception raised in Kafka topic subscribe\n")

    try:
        while True:
            # polling messages in Kafka topic "event_update"
            msg = consumer_kafka.poll(timeout=5.0)
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

                # each Kafka message is related to a single location, in order to reduce as much as
                # possible the number of OpenWeatherAPI query that worker must do, and it is a JSON
                # that contains many key-value pairs such as key = "location" and value = location
                # info in a list, then key = "user_id" and value = list of user_id of the users that are
                # interested in the location, and lastly many other key-value pairs with
                # key = rule_name and value = target value list for all the user according the order of
                # user id in user_id list

                # if a user is not interested in a specific rule for the location, then its rule value
                # corresponding to the user id is set at "null", while if no user is interested in a
                # specific rule for the location, then the key-value pair with key = rule name is not
                # in the Kafka message

                record_key = msg.key()
                logger.info("RECORD_KEY: " + str(record_key))
                record_value = msg.value()
                logger.info("RECORD_VALUE: " + str(record_value))
                data = json.loads(record_value)
                logger.info("DATA: " + str(data))
                # update current_work in DB
                userId_list = data.get("user_id")
                logger.info("USER_ID_LIST: " + str(userId_list))
                loc = data.get('location')
                logger.info("LOCATION: " + str(loc))
                try:
                    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                 user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                 database=os.environ.get('DATABASE')) as mydb:
                        # if the WMS is replicated and another message arrives identical to the one
                        # that has already arrived and for which the relevant entries have already
                        # been inserted in the table, no further insertion is made
                        mycursor = mydb.cursor()
                        for i in range(0, len(userId_list)):
                            temp_dict = dict()
                            for key in set(data.keys()):
                                if key != "location" and key != "rows_id":
                                    temp_dict[key] = data.get(key)[i]
                            temp_dict['location'] = loc
                            json_to_insert = json.dumps(temp_dict)
                            DBstart_time = time.time_ns()
                            mycursor.execute("INSERT INTO current_work (worker_id, rules, time_stamp) VALUES (%s, %s, CURRENT_TIMESTAMP())", (worker_id, json_to_insert))
                            DBend_time = time.time_ns()
                            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                        mydb.commit()  # to make changes effective after inserting rules for ALL the users
                except mysql.connector.Error as err:
                    logger.error("Exception raised! -> " + str(err) + "\n")
                    try:
                        mydb.rollback()
                    except Exception as exe:
                        logger.error(f"Exception raised in rollback: {exe}\n")
                    raise SystemExit

                # make commit
                try:
                    consumer_kafka.commit(asynchronous=True)
                except Exception as e:
                    logger.error("Error in commit! -> " + str(e) + "\n")
                    raise SystemExit

                # call to find_current_work and publish them in topic "event_to_be_sent"
                current_work = find_current_work()
                if current_work != '{}':  # JSON representation of an empty dictionary.
                    current_work_dict = json.loads(current_work)
                    current_work_dict['timestamp'] = time.time_ns()
                    current_work = json.dumps(current_work_dict)
                    while produce_kafka_message(topic, producer_kafka, current_work) == False:
                        pass

    except (KeyboardInterrupt, SystemExit):  # to terminate correctly with either CTRL+C or docker stop
        pass
    finally:
        # Leave group and commit final offsets
        consumer_kafka.close()

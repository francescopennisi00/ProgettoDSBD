import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic
import json
import mysql.connector
import os
import sys
import requests
# from datetime import datetime, timedelta


def commit_completed(er, partitions):
    if er:
        print(str(er))
    else:
        print("Commit done!\n")
        print("Committed partition offsets: " + str(partitions) + "\n")
        print("Rules fetched and stored in DB in order to save current work!\n")


def make_query(query):
    try:
        resp = requests.get(url=query)
        resp.raise_for_status()
        response = resp.json()
        if response.get('cod') != 200:
            raise Exception('Query failed: ' + response.get('message'))
        print(json.dumps(response) + "\n")
        return response
    except requests.JSONDecodeError as er:
        sys.stderr.write(f'JSON Decode error: {er}\n')
    except requests.HTTPError as er:
        sys.stderr.write(f'HTTP Error: {er}\n')
    except requests.exceptions.RequestException as er:
        sys.stderr.write(f'Request failed: {er}\n')
    except Exception as er:
        sys.stderr.write(f'Error: {er}\n')


# compare values obtained from OpenWeather API call with those that have been placed into the DB
# for recoverability from faults that occur before to possibly publish violated rules
# returns the violated rules to be sent in the form of a dictionary that contains many other
# dictionary with key = user_id and value = the list of (violated rule-current value) pairs
# there is another key-value pair in the outer dictionary with key = "location" and value = array
# that contains information about the location in common for all the entries to be entered into the DB
def check_rules(db_cursor, api_response):
    db_cursor.execute("SELECT rules FROM current_work WHERE worker_id = %s", (str(worker_id),))
    rules_list = db_cursor.fetchall()
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
        sys.stderr.write("Exception raised! -> " + str(error) + "\n")
        try:
            db_conn.rollback()
        except Exception as ex:
            sys.stderr.write(f"Exception raised in rollback: {ex}\n")
        return False
    return events_to_be_sent


# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
        raise SystemExit("Exiting after error in delivery message to Kafka broker\n")
    else:
        sys.stderr.write('%% Message delivered to %s, partition[%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))
        try:
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as mydb:
                mycursor = mydb.cursor()
                mycursor.execute("DELETE FROM current_work WHERE worker_id = %s", (str(worker_id),))
                mydb.commit()  # to make changes effective
        except mysql.connector.Error as err:
            sys.stderr.write("Exception raised!\n" + str(err))
            try:
                mydb.rollback()
            except Exception as exe:
                sys.stderr.write(f"Exception raised in rollback: {exe}\n")
            raise SystemExit


def produce_kafka_message(topic_name, kafka_producer, message):
    # Publish on the specific topic
    try:
        kafka_producer.produce(topic_name, value=message, callback=delivery_callback)
    except BufferError:
        sys.stderr.write(
            '%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(kafka_producer))
        return False
    # Wait until the message have been delivered
    sys.stderr.write("Waiting for message to be delivered\n")
    kafka_producer.flush()
    return True


worker_id = os.environ.get(("WORKERID"))


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
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()
            # we need worker_id in order to insert and remove only current work of worker replica
            # and don't affect current work of other replicas
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS current_work (id INTEGER PRIMARY KEY AUTO_INCREMENT, rules JSON NOT NULL, time_stamp TIMESTAMP NOT NULL, worker_id INTEGER NOT NULL, INDEX worker_ind (worker_id))")
            mydb.commit()  # to make changes effective
    except mysql.connector.Error as err:
        sys.stderr.write("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as exe:
            sys.stderr.write(f"Exception raised in rollback: {exe}\n")
        sys.exit("Exiting...\n")

    current_work = find_current_work()
    if current_work == False:
        sys.exit("Exiting after error in fetching rules to send\n")

    # Kafka admin and producer initialization in order to publish in topic "event_to_be_notified"
    broker = 'kafka:9092'
    topic = 'event_to_be_notified'
    producer_conf = {'bootstrap.servers': broker, 'acks': 1}
    admin_conf = {'bootstrap.servers': broker}
    kadmin = AdminClient(admin_conf)

    # Create topic "event_to_be_notified" if not exists
    list_topics_metadata = kadmin.list_topics()
    topics = list_topics_metadata.topics  # Returns a dict()
    print(f"LIST_TOPICS: {list_topics_metadata}")
    print(f"TOPICS: {topics}")
    topic_names = set(topics.keys())
    print(f"TOPIC_NAMES: {topic_names}")
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
        while produce_kafka_message(topic, producer_kafka, current_work) == False:
            pass
    else:
        print("There is no backlog of work\n")

    # start Kafka subscription
    consumer_kafka = confluent_kafka.Consumer(
        {'bootstrap.servers': 'kafka:9092', 'group.id': 'group2', 'enable.auto.commit': 'false',
         'auto.offset.reset': 'latest', 'on_commit': commit_completed})
    try:
        consumer_kafka.subscribe(['event_update'])  # the worker_service is also a Consumer related to the WMS Producer
    except confluent_kafka.KafkaException as ke:
        sys.stderr.write("Kafka exception raised! -> " + str(ke) + "\n")
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
                print("Waiting for message or event/error in poll()\n")
                continue
            elif msg.error():
                print('error: {}\n'.format(msg.error()))
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
                print("RECORD_KEY   " + str(record_key))
                record_value = msg.value()
                print("RECORD_VALUE   " + str(record_value))
                data = json.loads(record_value)
                print("DATA   " + str(data))
                # update current_work in DB
                userId_list = data.get("user_id")
                print("USER_ID_LIST   " + str(userId_list))
                loc = data.get('location')
                print("LOCATION   " + str(loc))
                try:
                    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                 user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                 database=os.environ.get('DATABASE')) as mydb:
                        # if the WMS is replicated and another message arrives identical to the one
                        # that has already arrived and for which the relevant entries have already
                        # been inserted in the table, no further insertion is made
                        mycursor = mydb.cursor()
                        # mycursor.execute("SELECT time_stamp FROM current_work")
                        # timestamp_list = mycursor.fetchone()
                        # if timestamp_list:
                        # timestamp_from_db = timestamp_list[0]
                        # current_timestamp = datetime.now()
                        # time_difference = current_timestamp - timestamp_from_db
                        # target_time_difference = timedelta(hours=1)
                        # if time_difference > target_time_difference:
                        for i in range(0, len(userId_list)):
                            temp_dict = dict()
                            for key in set(data.keys()):
                                if key != "location" and key != "rows_id":
                                    temp_dict[key] = data.get(key)[i]
                            temp_dict['location'] = loc
                            json_to_insert = json.dumps(temp_dict)
                            mycursor.execute("INSERT INTO current_work (worker_id, rules, time_stamp) VALUES (%s, %s, CURRENT_TIMESTAMP())", (worker_id, json_to_insert))
                        mydb.commit()  # to make changes effective after inserting rules for ALL the users
                except mysql.connector.Error as err:
                    sys.stderr.write("Exception raised! -> " + str(err) + "\n")
                    try:
                        mydb.rollback()
                    except Exception as exe:
                        sys.stderr.write(f"Exception raised in rollback: {exe}\n")
                    raise SystemExit

                # make commit
                try:
                    consumer_kafka.commit(asynchronous=True)
                except Exception as e:
                    sys.stderr.write("Error in commit! -> " + str(e) + "\n")
                    raise SystemExit

                # call to find_current_work and publish them in topic "event_to_be_sent"
                current_work = find_current_work()
                if current_work != '{}':  # JSON representation of an empty dictionary.
                    while produce_kafka_message(topic, producer_kafka, current_work) == False:
                        pass

    except (KeyboardInterrupt, SystemExit):  # to terminate correctly with either CTRL+C or docker stop
        pass
    finally:
        # Leave group and commit final offsets
        consumer_kafka.close()

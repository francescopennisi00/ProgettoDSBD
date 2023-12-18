import confluent_kafka
import json
import mysql.connector
import os
import sys
import requests


def commit_completed(er):
    if er:
        print(str(er))
    else:
        print("Notification fetched and stored in DB in order to be sent!")


def make_query(query):
    try:
        resp = requests.get(url=query)
        resp.raise_for_status()
        data = resp.json()
        if data.get('cod') != 200:
            raise Exception('Query failed: ' + data.get('message'))
        print(json.dumps(data))
        return data
    except requests.JSONDecodeError as e:
        print(f'JSON Decode error: {e}')
    except requests.HTTPError as e:
        print(f'HTTP Error: {e}')
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
    except Exception as e:
        print(f'Error: {e}')


# check values obtained from OpenWeather API call and put into the DB for recoverability from faults
# returns the violated_rules to be sent in the form of a dictionary with key user_id
# and value the list of (violated rule-current value) pairs
def check_rules(db_cursor, api_response):
    db_cursor.execute("SELECT rules FROM current_works")
    rules_list = db_cursor.fetchall()
    event_dict = dict()
    for rules in rules_list:
        user_violated_rules_list = list()
        rules_json = json.loads(rules[0])
        keys_set_target = set(rules_json.keys())
        for key in keys_set_target:
            temp_dict = dict()
            if "max" in key:
                if api_response.get(key) > rules_json.get(key):
                    temp_dict[key] = api_response.get(key)
            elif "min" in key:
                if api_response.get(key) < rules_json.get(key):
                    temp_dict[key] = api_response.get(key)
            # if "rain" and "snow" are in keys_set_target then these rules are of interest to the user
            # so rules_json.get("rain"/"snow") is surely True
            elif key == "rain" and api_response.get("rain") == True:
                temp_dict[key] = api_response.get(key)
            elif key == "snow" and api_response.get("snow") == True:
                temp_dict[key] = api_response.get(key)
            elif key == "wind_direction" and rules_json.get(key) == api_response.get(key):
                temp_dict[key] = api_response.get(key)
            user_violated_rules_list.append(temp_dict)
        event_dict[rules.get("user_id")] = user_violated_rules_list
    event_dict['location'] = rules_list[0].get('location')
    return json.dumps(event_dict)  # all entries in rules_list have the same location


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
    output_json_dict["clouds_max"] = data["clouds"]["all"]
    output_json_dict["clouds_min"] = data["clouds"]["all"]
    direction_list = ["N", "NE", "E", " SE", "S", "SO", "O", "NO"]
    j = 0
    for i in range(0, 316, 45):
        if (i - 22.5) <= data["wind"]["deg"] <= (i + 22.5):
            output_json_dict["wind_direction"] = direction_list[j]
            break
        j = j + 1
    if data["weather"]["main"] == "Rain":
        output_json_dict["rain"] = True
    else:
        output_json_dict["rain"] = False
    if data["weather"]["main"] == "Snow":
        output_json_dict["snow"] = True
    else:
        output_json_dict["snow"] = False

    return output_json_dict


# function for recovering unchecked rules when worker goes down before publishing notification event
def find_current_works():
    try:
        with (mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                      user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                      database=os.environ.get('DATABASE')) as db_conn):
            db_cursor = db_conn.cursor()
            db_cursor.execute("SELECT rules FROM current_works")
            results = db_cursor.fetchall()
            for row in results:
                location = row[0].get('location')
                # make OpenWeather API call
                apikey = os.environ.get('APIKEY')
                rest_call = f"https://api.openweathermap.org/data/2.5/weather?lat={location[1]}&lon={location[2]}&appid={apikey}"
                data = make_query(rest_call)
                formatted_data = format_data(data)
                events_to_be_sent = check_rules(db_cursor, formatted_data)
    except mysql.connector.Error as error:
        print("Exception raised!\n" + str(error))
        try:
            db_conn.rollback()
        except Exception as ex:
            print(f"Exception raised in rollback: {ex}")
        return False
    return events_to_be_sent


def produce_kafka_message(topic, producer_kafka, current_works):
    # Publish on the specific topic
    try:
        producer_kafka.produce(topic, value=current_works, callback=delivery_callback)
    except BufferError:
        sys.stderr.write(
            '%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(producer_kafka))
        return False

    producer_kafka.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(producer_kafka))
    producer_kafka.flush()
    return True


if __name__ == "__main__":

    # create table current_works if not exists
    try:
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS currents_works (id INTEGER PRIMARY KEY AUTO_INCREMENT, rules JSON NOT NULL, time_stamp TIMESTAMP NOT NULL)")
            mydb.commit()  # to make changes effective
    except mysql.connector.Error as err:
        print("Exception raised!\n" + str(err))
        try:
            mydb.rollback()
        except Exception as exe:
            print(f"Exception raised in rollback: {exe}")
        sys.exit("Exiting...")

    current_works, location = find_current_works()
    if current_works == False:
        sys.exit("Exiting after error in fetching rules to send")

    # Kafka producer initialization in order to publish in topic "event_to_be_sent"
    broker = 'localhost:29092'
    topic = 'event_to_be_notified'
    conf = {'bootstrap.servers': broker, 'acks': 1}

    # Create Producer instance
    producer_kafka = confluent_kafka.Producer(**conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s, partition[%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
            try:
                with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                             user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                             database=os.environ.get('DATABASE')) as mydb:
                    mycursor = mydb.cursor()
                    mycursor.execute("DELETE * FROM current_works")
                    mydb.commit()  # to make changes effective
            except mysql.connector.Error as err:
                print("Exception raised!\n" + str(err))
                try:
                    mydb.rollback()
                except Exception as exe:
                    print(f"Exception raised in rollback: {exe}")
                sys.exit("Exiting...")


    # check if current works are pending and if it is true publish them to Kafka
    if current_works != '{}':  # JSON representation of an empty dictionary.
        produce_kafka_message(topic, producer_kafka, current_works)
    else:
        print("There is no backlog of work")

    # start Kafka subscription
    consumer_kafka = confluent_kafka.Consumer(
        {'bootstrap.servers': 'kafka:29092', 'group.id': 'group2', 'enable.auto.commit': 'false',
         'auto.offset.reset': 'latest', 'on_commit': commit_completed})
    try:
        consumer_kafka.subscribe(['event_update'])  # the worker_service is also a Consumer related to the WMS Producer
    except confluent_kafka.KafkaException as ke:
        print("Kafka exception raised!\n" + str(ke))
        consumer_kafka.close()
        sys.exit("Terminate after Exception raised in Kafka topic subscribe")

    try:
        while True:
            # polling messages in Kafka topic "event_update"
            msg = consumer_kafka.poll(timeout=5.0)
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

                # update current_works and location (if required) in DB
                userId_list = data.get("user_id")
                loc = data.get('location')
                for i in range(0, len(userId_list)):
                    temp_dict = dict()
                    for key in set(data.keys()):
                        if key != "location":
                            temp_dict[key] = data.get(key)[i]
                    temp_dict['location'] = loc
                    json_to_insert = json.dumps(temp_dict)
                    try:
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:
                            mycursor = mydb.cursor()
                            mycursor.execute("INSERT INTO current_works (rules) VALUES (%s)", (json_to_insert,))
                            mydb.commit()  # to make changes effective
                    except mysql.connector.Error as err:
                        print("Exception raised!\n" + str(err))
                        try:
                            mydb.rollback()
                        except Exception as exe:
                            print(f"Exception raised in rollback: {exe}")
                    sys.exit("Exiting...")

            # Kafka commit
                # make commit
                try:
                    consumer_kafka.commit(asynchronous=True)
                    print("Commit done!")
                except Exception as e:
                    print("Error in commit!\n" + str(e))
                    raise SystemExit

                # call to find_current_works and publish them in topic "event_to_be_sent"
                current_works = find_current_works()
                result = produce_kafka_message(topic,producer_kafka,current_works)
                if result == False:
                    sys.stderr.write("Error")
                    continue


    except (KeyboardInterrupt, SystemExit):  # to terminate correctly with either CTRL+C or docker stop
        pass
    finally:
        # Leave group and commit final offsets
        consumer_kafka.close()

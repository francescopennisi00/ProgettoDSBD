import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic
import json
import grpc
# import WMS_um_pb2
# import WMS_um_pb2_grpc
# import WMS_apigateway_pb2_grpc TODO: maybe to remove
# import WMS_apigateway_pb2 TODO: maybe to remove
import mysql.connector
import os
import time
import sys
import threading
from concurrent import futures


def make_kafka_message(final_json_dict, location_id, mycursor):
    mycursor.execute("SELECT location_name, lat, long, country_code, state_code FROM location WHERE id = %s", (str(location_id), ))
    location = mycursor.fetchone()  # list of information about current location of the Kafka message
    userid_list = list()
    max_temp_list = list()
    min_temp_list = list()
    max_humidity_list = list()
    min_humidity_list = list()
    max_pressure_list = list()
    min_pressure_list = list()
    max_cloud_list = list()
    min_cloud_list = list()
    max_wind_speed_list = list()
    min_wind_speed_list = list()
    wind_direction_list = list()
    rain_list = list()
    snow_list = list()
    rows_id_list = list()
    mycursor.execute("SELECT * FROM user_constraints WHERE TIMESTAMPDIFF(HOUR, CURRENT_TIMESTAMP(), time_stamp) > trigger_period AND location_id = %s", (str(location_id),))
    results = mycursor.fetchall()
    for result in results:
        rules_dict = json.loads(result[3])
        userid_list.append(result[1])
        rows_id_list.append(result[0])
        max_temp_list.append(rules_dict.get("max_temp"))
        min_temp_list.append(rules_dict.get("min_temp"))
        max_humidity_list.append(rules_dict.get("max_humidity"))
        min_humidity_list.append(rules_dict.get("min_humidity"))
        max_pressure_list.append(rules_dict.get("max_pressure"))
        min_pressure_list.append(rules_dict.get("min_pressure"))
        max_cloud_list.append(rules_dict.get("max_cloud"))
        min_cloud_list.append(rules_dict.get("min_cloud"))
        max_wind_speed_list.append(rules_dict.get("max_wind_speed"))
        min_wind_speed_list.append(rules_dict.get("min_wind_speed"))
        wind_direction_list.append(rules_dict.get("wind_direction"))
        rain_list.append(rules_dict.get("rain"))
        snow_list.append(rules_dict.get("snow"))

    final_json_dict["rows_id"] = rows_id_list
    final_json_dict['user_id'] = userid_list
    final_json_dict['location'] = location
    final_json_dict['max_temp'] = max_temp_list
    final_json_dict['min_temp'] = min_temp_list
    final_json_dict['max_humidity'] = max_humidity_list
    final_json_dict['min_humidity'] = min_humidity_list
    final_json_dict['max_pressure'] = max_pressure_list
    final_json_dict['min_pressure'] = min_pressure_list
    final_json_dict['max_cloud'] = max_cloud_list
    final_json_dict['min_cloud'] = min_cloud_list
    final_json_dict['max_wind_speed'] = max_wind_speed_list
    final_json_dict['min_wind_speed'] = min_wind_speed_list
    final_json_dict['rain'] = rain_list
    final_json_dict['snow'] = snow_list

    return json.dumps(final_json_dict)


# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
# Updating table user_constraints in order to avoid considering again a row in the building of
# Kafka message to publish in "event_update" topic. In this way, we prevent multiple replication of
# the WMS from sending the same trigger message to worker(s)
def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
        raise SystemExit("Exiting after error in delivery message to Kafka broker\n")
    else:
        sys.stderr.write('%% Message delivered to %s, partition[%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))
        message_dict = json.loads(msg)
        rows_id_list = message_dict.get("rows_id")
        try:
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as mydb:
                mycursor = mydb.cursor()
                for id in rows_id_list:
                    mycursor.execute("UPDATE user_constraints SET time_stamp = CURRENT_TIMESTAMP() WHERE id = %s", (str(id), ))
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


def find_pending_work():
    try:
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:

            # buffered=True needed because we reuse mycursor after a fetchone() in called function
            mycursor = mydb.cursor(buffered=True)

            # retrieve all the information about locations to build Kafka messages
            mycursor.execute("SELECT location_id FROM user_constraints WHERE TIMESTAMPDIFF(HOUR, CURRENT_TIMESTAMP(), time_stamp) > trigger_period GROUP BY location_id")
            location_id_list = mycursor.fetchall()
            Kafka_message_list = list()
            for location in location_id_list:
                location_id = location[0]
                final_json_dict = dict()
                message = make_kafka_message(final_json_dict, location_id, mycursor)
                Kafka_message_list.append(message)
            return Kafka_message_list

    except mysql.connector.Error as err:
        sys.stderr.write("Exception raised! -> " + str(err) + "\n")
        return False


def timer(interval, event):
    time.sleep(interval)  # every hour the timer thread wakes up the main thread in order to send update
    event.set()


def serve_apigateway():
    port = '50051'
    # server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    # WMS_apigateway_pb2_grpc.add_WMSAPIGatewayServicer_to_server(WMSAPIGateway(), server)
    # server.add_insecure_port('[::]:' + port)
    # server.start()
    # TODO: to be reviewed with REST, not gRPC
    print("API Gateway thread server started, listening on " + port + "\n")
    # server.wait_for_termination()



if __name__ == "__main__":

    # setting env variables for secrets
    secret_password_path = os.environ.get('PASSWORD')
    with open(secret_password_path, 'r') as file:
        secret_password_value = file.read()
    os.environ['PASSWORD'] = secret_password_value

    # create tables location and user_constraints if not exists.
    try:
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()
            mycursor.execute("CREATE TABLE IF NOT EXISTS location (id INTEGER PRIMARY KEY AUTO_INCREMENT, location_name VARCHAR(100) NOT NULL, lat FLOAT NOT NULL, long FLOAT NOT NULL, country_code VARCHAR(10) NOT NULL, state_code VARCHAR(70) NOT NULL)")
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS user_constraints (id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER NOT NULL, location_id INTEGER NOT NULL, rules JSON NOT NULL, time_stamp TIMESTAMP NOT NULL, trigger_period INTEGER NOT NULL, FOREIGN KEY location_id REFERENCES location(id), UNIQUE KEY user_location_id (user_id, location_id))")
            mydb.commit()  # to make changes effective
    except mysql.connector.Error as err:
        sys.stderr.write("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as exe:
            sys.stderr.write(f"Exception raised in rollback: {exe}\n")
        sys.exit("Exiting...\n")

    # Kafka admin and producer initialization in order to publish in topic "event_update"
    broker = 'kafka:9092'
    topic = 'event_update'
    producer_conf = {'bootstrap.servers': broker, 'acks': 1}
    admin_conf = {'bootstrap.servers': broker}
    kadmin = AdminClient(admin_conf)

    # Create topic "event_update" if not exists
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

    # check in DB in order to find events to send
    Kafka_msg_list = find_pending_work()
    if Kafka_msg_list != False:
        for message in Kafka_msg_list:
            produce_kafka_message(topic, producer_kafka, message)
    else:
        sys.exit("Error in finding pending work!")

    # Event object for thread communication
    expired_timer_event = threading.Event()

    print("Starting timer thread!\n")
    threadTimer = threading.Thread(target=timer(3600, expired_timer_event))
    threadTimer.daemon = True
    print("Starting API Gateway serving thread!\n")
    threadAPIGateway = threading.Thread(target=serve_apigateway())
    threadAPIGateway.daemon = True

    while True:
        # wait for expired timer event
        expired_timer_event.wait()
        # check in DB in order to find events to send
        Kafka_msg_list = find_pending_work()
        if Kafka_msg_list != False:
            for message in Kafka_msg_list:
                produce_kafka_message(topic, producer_kafka, message)
        else:
            sys.exit("Error in finding pending work!")

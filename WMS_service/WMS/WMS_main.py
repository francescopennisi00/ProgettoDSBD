import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic
import json
from concurrent import futures
import grpc
import WMS_um_pb2
import WMS_um_pb2_grpc
import mysql.connector
import os
import time
import sys
import threading
from flask import Flask
from flask import request
import socket

# create lock objects for mutual exclusion in acquire stdout and stderr resource
lock = threading.Lock()
lock_error = threading.Lock()


def safe_print(message):
    with lock:
        print(message)


def safe_print_error(error):
    with lock_error:
        sys.stderr.write(error)


class WMSUm(WMS_um_pb2_grpc.WMSUmServicer):
    def RequestDeleteUser_Constraints(self, request, context):
        user_id = request.user_id
        try:
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as db:
                cursor = db.cursor()
                cursor.execute("DELETE FROM user_constraints WHERE user_id= %s", (user_id,))
                db.commit()
                return WMS_um_pb2.Response_Code(response_code=200)
        except mysql.connector.Error as error:
            safe_print_error("Exception raised! -> {0}".format(str(error)))
            try:
                mydb.rollback()
            except Exception as exe:
                sys.stderr.write(f"Exception raised in rollback: {exe}\n")
            return WMS_um_pb2.Response_Code(response_code=-1)

def make_kafka_message(final_json_dict, location_id, mycursor):
    mycursor.execute("SELECT location_name, latitude, longitude, country_code, state_code FROM locations WHERE id = %s",
                     (str(location_id),))
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
    mycursor.execute(
        "SELECT * FROM user_constraints WHERE TIMESTAMPDIFF(SECOND,  time_stamp, CURRENT_TIMESTAMP()) > trigger_period AND location_id = %s",
        (str(location_id),))
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

    # if no user is interested in a particular rule,
    # then insertion of relative list of null values is not made

    found = False
    for element in max_temp_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['max_temp'] = max_temp_list

    found = False
    for element in min_temp_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['min_temp'] = min_temp_list

    found = False
    for element in max_humidity_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['max_humidity'] = max_humidity_list

    found = False
    for element in min_humidity_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['min_humidity'] = min_humidity_list

    found = False
    for element in max_pressure_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['max_pressure'] = max_pressure_list

    found = False
    for element in min_pressure_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['min_pressure'] = min_pressure_list

    found = False
    for element in max_cloud_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['max_cloud'] = max_cloud_list

    found = False
    for element in min_cloud_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['min_cloud'] = min_cloud_list

    found = False
    for element in max_wind_speed_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['max_wind_speed'] = max_wind_speed_list

    found = False
    for element in min_wind_speed_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['min_wind_speed'] = min_wind_speed_list

    found = False
    for element in rain_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['rain'] = rain_list

    found = False
    for element in snow_list:
        if element != "null":
            found = True
            break
    if found == True:
        final_json_dict['snow'] = snow_list
    safe_print("FINAL JSON DICT" + str(final_json_dict))
    return json.dumps(final_json_dict)


# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
# Updating table user_constraints in order to avoid considering again a row in the building of
# Kafka message to publish in "event_update" topic. In this way, we prevent multiple replication of
# the WMS from sending the same trigger message to worker(s)
def delivery_callback(err, msg):
    if err:
        safe_print_error('%% Message failed delivery: %s\n' % err)
        raise SystemExit("Exiting after error in delivery message to Kafka broker\n")
    else:
        safe_print_error('%% Message delivered to %s, partition[%d] @ %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))
        message_dict = json.loads(msg.value())
        safe_print(message_dict)
        rows_id_list = message_dict.get("rows_id")
        try:
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as mydb:
                mycursor = mydb.cursor()
                for id in rows_id_list:
                    safe_print("ID in ROWS_ID_LIST  " + str(id))
                    mycursor.execute("UPDATE user_constraints SET time_stamp = CURRENT_TIMESTAMP() WHERE id = %s",
                                     (str(id),))
                mydb.commit()  # to make changes effective
        except mysql.connector.Error as err:
            safe_print_error("Exception raised!\n" + str(err))
            try:
                mydb.rollback()
            except Exception as exe:
                safe_print_error(f"Exception raised in rollback: {exe}\n")
            raise SystemExit


def produce_kafka_message(topic_name, kafka_producer, message):
    # Publish on the specific topic
    try:
        kafka_producer.produce(topic_name, value=message, callback=delivery_callback)
    except BufferError:
        safe_print_error(
            '%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(kafka_producer))
        return False
    # Wait until the message have been delivered
    safe_print_error("Waiting for message to be delivered\n")
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
            mycursor.execute(
                "SELECT location_id FROM user_constraints WHERE TIMESTAMPDIFF(SECOND,  time_stamp, CURRENT_TIMESTAMP()) > trigger_period GROUP BY location_id")
            location_id_list = mycursor.fetchall()
            Kafka_message_list = list()
            for location in location_id_list:
                location_id = location[0]
                final_json_dict = dict()
                message = make_kafka_message(final_json_dict, location_id, mycursor)
                Kafka_message_list.append(message)
            return Kafka_message_list

    except mysql.connector.Error as err:
        safe_print_error("Exception raised! -> " + str(err) + "\n")
        return False


def timer(interval, event):
    while True:
        time.sleep(interval)  # every hour the timer thread wakes up the main thread in order to send update
        event.set()


def authenticate_and_retrieve_user_id(header):
    jwt_token = header.split(' ')[1]  # Extract token from "Bearer <token>" string

    # start gRPC communication with user_manager in order to retrieve user id
    try:
        with grpc.insecure_channel('um_service:50052') as channel:
            stub = WMS_um_pb2_grpc.WMSUmStub(channel)
            response = stub.RequestUserIdViaJWTToken(WMS_um_pb2.Request(jwt_token=jwt_token))
            safe_print("Fetched user id: " + str(response.user_id) + "\n")
            user_id_to_return = response.user_id  # user id < 0 if some error occurred
    except grpc.RpcError as error:
        safe_print_error("gRPC error! -> " + str(error) + "\n")
        user_id_to_return = "null"
    return user_id_to_return


def create_app():
    app = Flask(__name__)

    @app.route('/update_rules/delete_user_constraints_by_location', methods=['POST'])
    def delete_user_constraints_by_location_handler():
        # Verify if data received is a JSON
        if request.is_json:
            try:
                # Extract json data
                data_dict = request.get_json()
                safe_print("DELETE USER CONSTRAINTS BY LOCATION \n\n Data received: " + str(data_dict))
                if data_dict:
                    # Communication with UserManager in order to authenticate the user and retrieve user_id
                    authorization_header = request.headers.get('Authorization')
                    if authorization_header and authorization_header.startswith('Bearer '):
                        id_user = authenticate_and_retrieve_user_id(authorization_header)
                        if id_user == "null":
                            return 'Error in communication with authentication server: retry!', 500
                        elif id_user == -1:
                            return 'JWT Token expired: login required!', 401
                        elif id_user == -2:
                            return 'Error in communication with DB in order to authentication: retry!', 500
                        elif id_user == -3:
                            return 'JWT Token is not valid: login required!', 401
                    else:
                        # No token provided in authorization header
                        return 'JWT Token not provided: login required!', 401
                    location_name = data_dict.get('location')[0]
                    latitude = data_dict.get('location')[1]
                    rounded_latitude = round(latitude, 3)
                    longitude = data_dict.get('location')[2]
                    rounded_longitude = round(longitude, 3)
                    country_code = data_dict.get('location')[3]
                    state_code = data_dict.get('location')[4]
                    safe_print_error(
                        "LOCATION  " + location_name + '  ' + str(rounded_latitude) + '  ' + str(rounded_longitude) + '  ' + str(
                            country_code) + '  ' + str(state_code) + "\n\n")
                    str_json = json.dumps(data_dict)
                    try:
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:

                            # buffered=True needed because we reuse mycursor after a fetchone()
                            mycursor = mydb.cursor(buffered=True)

                            # retrieve all the information about locations to build Kafka messages
                            mycursor.execute(
                                "SELECT * FROM locations WHERE ROUND(latitude,3) = %s and ROUND(longitude,3) = %s and location_name = %s",
                                (str(rounded_latitude), str(rounded_longitude), location_name))
                            row = mycursor.fetchone()
                            if not row:
                                safe_print_error("There is no entry with that latitude and longitude\n")
                                return "Error, there is no locations to delete with that parameters", 400
                            else:
                                location_id = row[0]
                                mycursor.execute("DELETE FROM user_constraints WHERE user_id = %s and location_id = %s",
                                                 (str(id_user), str(location_id)))
                                mydb.commit()
                                return "Row in user_constraints correctly deleted", 200
                    except mysql.connector.Error as err:
                        safe_print_error("Exception raised! -> " + str(err) + "\n")
                        try:
                            mydb.rollback()
                        except Exception as exe:
                            sys.stderr.write(f"Exception raised in rollback: {exe}\n")
                        return f"Error in connecting to database: {str(err)}", 500

            except Exception as e:
                return f"Error in reading data: {str(e)}", 400
        else:
            return "Error: the request must be in JSON format", 400

    @app.route('/update_rules', methods=['POST'])
    def update_rules_handler():
        # Verify if data received is a JSON
        if request.is_json:
            try:
                # Extract json data
                data_dict = request.get_json()
                print("Data received:" + str(data_dict))
                if data_dict:
                    # Communication with UserManager in order to authenticate the user and retrieve user_id
                    authorization_header = request.headers.get('Authorization')
                    if authorization_header and authorization_header.startswith('Bearer '):
                        id_user = authenticate_and_retrieve_user_id(authorization_header)
                        if id_user == "null":
                            return 'Error in communication with authentication server: retry!', 500
                        elif id_user == -1:
                            return 'JWT Token expired: login required!', 401
                        elif id_user == -2:
                            return 'Error in communication with DB in order to authentication: retry!', 500
                        elif id_user == -3:
                            return 'JWT Token is not valid: login required!', 401
                    else:
                        # No token provided in authorization header
                        return 'JWT Token not provided: login required!', 401
                    trigger_period = data_dict.get('trigger_period')
                    location_name = data_dict.get('location')[0]
                    latitude = data_dict.get('location')[1]
                    rounded_latitude = round(latitude, 3)
                    longitude = data_dict.get('location')[2]
                    rounded_longitude = round(longitude, 3)
                    country_code = data_dict.get('location')[3]
                    state_code = data_dict.get('location')[4]
                    safe_print_error(
                        "LOCATION  " + location_name + '  ' + str(rounded_latitude) + '  ' + str(rounded_longitude) + '  ' + str(
                            country_code) + '  ' + str(state_code) + "\n\n")
                    del data_dict['trigger_period']
                    str_json = json.dumps(data_dict)
                    try:
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:

                            # buffered=True needed because we reuse mycursor after a fetchone()
                            mycursor = mydb.cursor(buffered=True)

                            # retrieve all the information about locations to build Kafka messages
                            mycursor.execute(
                                "SELECT * FROM locations WHERE ROUND(latitude,3) = %s and ROUND(longitude,3) = %s and location_name = %s",
                                (str(rounded_latitude), str(rounded_longitude), location_name))
                            row = mycursor.fetchone()
                            if not row:
                                safe_print_error("There is no entry with that latitude and longitude\n")
                                mycursor.execute(
                                    "INSERT INTO locations (location_name, latitude, longitude, country_code, state_code) VALUES (%s, %s, %s, %s, %s)",
                                    (location_name, str(rounded_latitude), str(rounded_longitude), country_code,
                                     state_code))
                                mydb.commit()
                                location_id = mycursor.lastrowid
                                safe_print("New location correctly inserted!\n")
                            else:
                                location_id = row[0]  # location id = first element of first

                            mycursor.execute("SELECT * FROM user_constraints WHERE user_id = %s and location_id = %s",
                                             (str(id_user), str(location_id)))
                            result = mycursor.fetchone()
                            if result:
                                mycursor.execute(
                                    "UPDATE user_constraints SET rules = %s WHERE user_id = %s and location_id = %s",
                                    (str_json, str(id_user), str(location_id)))
                                mydb.commit()
                                mycursor.execute(
                                    "UPDATE user_constraints SET trigger_period = %s WHERE user_id = %s and location_id = %s",
                                    (str(trigger_period), str(id_user), str(location_id)))
                                mydb.commit()
                                return "Updated table user_constraints correctly!", 200
                            else:
                                mycursor.execute(
                                    "INSERT INTO user_constraints (user_id, location_id, rules, time_stamp, trigger_period) VALUES(%s, %s, %s, CURRENT_TIMESTAMP, %s)",
                                    (str(id_user), str(location_id), str_json, str(trigger_period)))
                                mydb.commit()
                                return "New user_constraints correctly inserted!", 200

                    except mysql.connector.Error as err:
                        safe_print_error("Exception raised! -> " + str(err) + "\n")
                        try:
                            mydb.rollback()
                        except Exception as exe:
                            sys.stderr.write(f"Exception raised in rollback: {exe}\n")
                        return f"Error in connecting to database: {str(err)}", 500

            except Exception as e:
                return f"Error in reading data: {str(e)}", 400
        else:
            return "Error: the request must be in JSON format", 400



    return app


# create Flask application
app = create_app()


def serve_apigateway():
    port = 50051
    hostname = socket.gethostname()
    safe_print(f'Hostname: {hostname} -> server starting on port {str(port)}')
    app.run(host='0.0.0.0', port=port, threaded=True)


def serve_um():
    port = '50052'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    WMS_um_pb2_grpc.add_WMSUmServicer_to_server(WMSUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    safe_print("UM thread server started, listening on " + port + "\n")
    server.wait_for_termination()


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
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS locations (id INTEGER PRIMARY KEY AUTO_INCREMENT, location_name VARCHAR(100) NOT NULL, latitude FLOAT NOT NULL, longitude FLOAT NOT NULL, country_code VARCHAR(10) NOT NULL, state_code VARCHAR(70) NOT NULL, UNIQUE KEY location_tuple (location_name, latitude, longitude));")
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS user_constraints (id INTEGER PRIMARY KEY AUTO_INCREMENT, user_id INTEGER NOT NULL, location_id INTEGER NOT NULL, rules JSON NOT NULL, time_stamp TIMESTAMP NOT NULL, trigger_period INTEGER NOT NULL, FOREIGN KEY (location_id) REFERENCES locations(id), UNIQUE KEY user_location_id (user_id, location_id));")
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
        kadmin.create_topics([new_topic, ])

    # Create Producer instance
    producer_kafka = confluent_kafka.Producer(**producer_conf)

    # check in DB in order to find events to send
    Kafka_msg_list = find_pending_work()
    if Kafka_msg_list != False:
        for message in Kafka_msg_list:
            while produce_kafka_message(topic, producer_kafka, message) == False:
                pass
    else:
        sys.exit("Error in finding pending work!")

    # Event object for thread communication
    expired_timer_event = threading.Event()

    safe_print("Starting timer thread!\n")
    threadTimer = threading.Thread(target=timer, args=(60, expired_timer_event))
    threadTimer.daemon = True
    threadTimer.start()
    safe_print("Starting API Gateway serving thread!\n")
    threadAPIGateway = threading.Thread(target=serve_apigateway)
    threadAPIGateway.daemon = True
    threadAPIGateway.start()
    threadUM = threading.Thread(target=serve_um)
    threadUM.daemon = True
    threadUM.start()

    while True:
        # wait for expired timer event
        expired_timer_event.wait()
        expired_timer_event.clear()
        # check in DB in order to find events to send
        Kafka_msg_list = find_pending_work()
        if Kafka_msg_list != False:
            for message in Kafka_msg_list:
                while produce_kafka_message(topic, producer_kafka, message) == False:
                    pass
        else:
            sys.exit("Error in finding pending work!")

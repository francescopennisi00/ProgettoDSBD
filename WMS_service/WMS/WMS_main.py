import confluent_kafka
import json
import grpc
import WMS_um_pb2
import WMS_um_pb2_grpc
import mysql.connector
import os
import time
import sys


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

def find_pending_work():
    try:
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()
            # retrieve all the information about locations to build Kafka messages
            # every message to be published in Kafka topic event_update is a JSON that contains a special
            # key-value pair with key = "location" and value = location info and other key-value pairs
            # that contains as values the list of ID of users that are interest of location and the list
            # of the rules specified
            mycursor.execute("SELECT location_id FROM user_constraints WHERE TIMESTAMPDIFF(HOUR, CURRENT_TIMESTAMP(), time_stamp) > trigger_period GROUP BY location_id")
            location_id_list = mycursor.fetchall()
            final_json_dict = dict()
            for row in location_id_list:
                mycursor.execute("SELECT location_name, lat, long, country_code, state_code FROM location WHERE id = %s", (str(row[0]), ))
                location = mycursor.fetchone()  # list of information about current location of the Kafka message
                userid_list = list()  # list of user id of users interested of current location
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
                mycursor.execute("SELECT * FROM user_constraints WHERE TIMESTAMPDIFF(HOUR, CURRENT_TIMESTAMP(), time_stamp) > trigger_period AND location_id = %s", (str(row[0]),))
                results = mycursor.fetchall()
                for result in results:
                    rules_dict = json.loads(result[3])
                    userid_list.append(result[1])
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

                final_json_dict['user_id'] = userid_list
                final_json_dict['location'] = location
                final_json_dict['max_temp'] = max_temp_list


                produce_kafka_message

            mydb.commit()  # to make changes effective
    except mysql.connector.Error as err:
        sys.stderr.write("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as exe:
            sys.stderr.write(f"Exception raised in rollback: {exe}\n")
        sys.exit("Exiting...\n")


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

    find_pending_work()
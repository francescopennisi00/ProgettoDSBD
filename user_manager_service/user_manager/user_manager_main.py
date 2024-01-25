import threading
import time
from concurrent import futures
import grpc
import WMS_um_pb2
import WMS_um_pb2_grpc
import notifier_um_pb2
import notifier_um_pb2_grpc
import mysql.connector
import os
import sys
import jwt
import socket
from flask import Flask
from flask import request
import hashlib
import datetime
from prometheus_client import Counter, generate_latest, REGISTRY, Gauge,Histogram
from flask import Response
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# definition of the metrics to be exposed
REQUEST = Counter('UM_requests', 'Total number of requests received by um-service')
FAILURE = Counter('UM_failure_requests', 'Total number of requests received by um-service that failed')
INTERNAL_ERROR = Counter('UM_internal_http_error', 'Total number of internal http errors in um-service')
RESPONSE_TO_WMS = Counter('UM_RESPONSE_TO_WMS', 'Total number of responses sent to wms-service')
RESPONSE_TO_NOTIFIER = Counter('UM_RESPONSE_TO_NOTIFIER', 'Total number of responses sent to notifier-service')
REGISTERED_USERS_COUNT = Gauge('UM_registered_users_count', 'Total number of registered users')
DELTA_TIME = Gauge('UM_response_time_client', 'Latency beetween instant in which client sends the API CALL and instant in which user-manager responses')
QUERY_DURATIONS_HISTOGRAM = Histogram('UM_query_durations_nanoseconds_DB', 'DB query durations in nanoseconds', buckets=[5000000, 10000000, 25000000, 50000000, 75000000, 100000000, 250000000, 500000000, 750000000, 1000000000, 2500000000,5000000000,7500000000,10000000000])
# buckets indicated because of measuring time in nanoseconds


class WMSUm(WMS_um_pb2_grpc.WMSUmServicer):

    # connection with DB and retrieve user id
    def RequestUserIdViaJWTToken(self, request, context):
        try:
            # extract token information without verifying them: needed in order to retrieve user email
            token_dict = jwt.decode(request.jwt_token, algorithms=['HS256'], options={"verify_signature": False})
            email = token_dict.get("email")
            try:
                DBstart_time = time.time_ns()
                with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                             user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                             database=os.environ.get('DATABASE')) as db:
                    cursor = db.cursor()
                    cursor.execute("SELECT id, password FROM users WHERE email= %s", (email,))
                    row = cursor.fetchone()
                    DBend_time = time.time_ns()
                    QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                    if row:
                        userid = row[0]
                        password = row[1]
                    else:
                        RESPONSE_TO_WMS.inc()
                        return WMS_um_pb2.Reply(user_id=-3)  # token is not valid: email not present
            except mysql.connector.Error as error:
                logger.error("Exception raised! -> {0}".format(str(error)))
                RESPONSE_TO_WMS.inc()
                return WMS_um_pb2.Reply(user_id=-2)
            # verify that password is correct verifying digital signature with secret = password
            jwt.decode(request.jwt_token, password, algorithms=['HS256'])
            RESPONSE_TO_WMS.inc()
            return WMS_um_pb2.Reply(user_id=userid)
        except jwt.ExpiredSignatureError:
            RESPONSE_TO_WMS.inc()
            return WMS_um_pb2.Reply(user_id=-1)  # token is expired
        except jwt.InvalidTokenError:
            RESPONSE_TO_WMS.inc()
            return WMS_um_pb2.Reply(user_id=-3)  # token is not valid: password incorrect


class NotifierUm(notifier_um_pb2_grpc.NotifierUmServicer):

    # connection with DB and retrieve email
    def RequestEmail(self, request, context):
        try:
            DBstart_time = time.time_ns()
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as db:
                cursor = db.cursor()
                cursor.execute("SELECT email FROM users WHERE id= %s", (str(request.user_id),))
                row = cursor.fetchone()
                DBend_time = time.time_ns()
                QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                if row:
                    email = row[0]
                else:
                    email = "not present anymore"
        except mysql.connector.Error as error:
            logger.error("Exception raised! -> {0}".format(str(error)))
            email = "null"
        RESPONSE_TO_NOTIFIER.inc()
        return notifier_um_pb2.Reply(email=email)


def serve_notifier():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_um_pb2_grpc.add_NotifierUmServicer_to_server(NotifierUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    logger.info("Notifier thread server started, listening on " + port + "\n")
    server.wait_for_termination()


def serve_wms():
    port = '50052'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    WMS_um_pb2_grpc.add_WMSUmServicer_to_server(WMSUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    logger.info("WMS thread server started, listening on " + port + "\n")
    server.wait_for_termination()


def calculate_hash(input_string):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(input_string.encode('utf-8'))
    hash_result = sha256_hash.hexdigest()
    return hash_result


def check_pending_metrics_to_restore(dbcursor):
    try:
        DBstart_time = time.time_ns()
        # check if are present metrics to restore into DB
        dbcursor.execute("SELECT metrics FROM metrics_to_restore")
        rows = dbcursor.fetchall()
        DBend_time = time.time_ns()
        QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
        for row in rows:
            json_data = row[0]
            try:
                with grpc.insecure_channel('wms-service:50052') as channel:
                    stub = WMS_um_pb2_grpc.WMSUmStub(channel)
                    response = stub.RestoreData(WMS_um_pb2.JsonEliminatedData(json_eliminated_data=json_data))
                    if response.code != -1:
                        return True
                    else:
                        return False
            except grpc.RpcError as error:
                logger.error("gRPC error! -> " + str(error) + "\n")
                return False
        return True  # if no metric is pending, return True because no action has to be done

    except mysql.connector.Error as err:
        logger.error("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as exe:
            logger.error(f"Exception raised in rollback: {exe}\n")
        return False


# implementing compensate function in order to re-insert into WMS DB the metric of user that has not been eliminated
# in delete_account Flask route handler, respecting SAGA pattern for distributed transaction
def compensate_user_constraints_elimination(json_to_restore):
    try:
        DBstart_time = time.time_ns()
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()

            # insert into DB metric to restore in order to prevent data loss caused by possible crashes
            mycursor.execute("INSERT INTO metrics_to_restore (metrics) VALUES (%s)", (json_to_restore,))
            mydb.commit()
            DBend_time = time.time_ns()
            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
            while check_pending_metrics_to_restore(mycursor) == False:
                pass
            return True
    except mysql.connector.Error as err:
        logger.error("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as exe:
            logger.error(f"Exception raised in rollback: {exe}\n")
        return False


def delete_UserConstraints_By_UserID(userId):
    try:
        with grpc.insecure_channel('wms-service:50052') as channel:
            stub = WMS_um_pb2_grpc.WMSUmStub(channel)
            response = stub.RequestDeleteUser_Constraints(WMS_um_pb2.User(user_id=userId))
            json_to_return = response.json_eliminated_data  # json_to_return = "error" if some error occurred
    except grpc.RpcError as error:
        logger.error("gRPC error! -> " + str(error) + "\n")
        json_to_return = "error"
    return json_to_return


def create_app():
    app = Flask(__name__)

    @app.route('/register', methods=['POST'])
    def user_register():
        # Increment wms_request metric
        REQUEST.inc()
        # Verify if data received is a JSON
        if request.is_json:
            try:
                # Extract json data
                data_dict = request.get_json()
                if data_dict:
                    email = data_dict.get("email")
                    logger.info("Email received:" + email)
                    password = data_dict.get("psw")
                    timestamp_client = data_dict.get("timestamp_client")
                    try:
                        DBstart_time = time.time_ns()
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:
                            mycursor = mydb.cursor()

                            # check if email already exists in DB
                            mycursor.execute("SELECT email FROM users WHERE email=%s", (email,))
                            email_row = mycursor.fetchone()
                            DBend_time = time.time_ns()
                            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                            if not email_row:
                                hash_psw = calculate_hash(password)  # we save hash in DB for major privacy for users
                                DBstart_time = time.time_ns()
                                mycursor.execute("INSERT INTO users (email, password) VALUES (%s,%s)",
                                                 (email, hash_psw))
                                mydb.commit()
                                DBend_time = time.time_ns()
                                QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                                REGISTERED_USERS_COUNT.inc()
                                DELTA_TIME.set(time.time_ns() - timestamp_client)
                                return "Registration made successfully! Now try to sign in!", 200
                            DELTA_TIME.set(time.time_ns() - timestamp_client)
                            FAILURE.inc()
                            return f"Email already in use! Try to sign in!", 401

                    except mysql.connector.Error as err:
                        logger.error("Exception raised! -> " + str(err) + "\n")
                        try:
                            mydb.rollback()
                        except Exception as exe:
                            logger.error(f"Exception raised in rollback: {exe}\n")
                        FAILURE.inc()
                        INTERNAL_ERROR.inc()
                        DELTA_TIME.set(time.time_ns() - timestamp_client)
                        return f"Error in connecting to database: {str(err)}", 500

            except Exception as e:
                FAILURE.inc()
                return f"Error in reading data: {str(e)}", 400
        else:
            FAILURE.inc()
            return "Error: the request must be in JSON format", 400

    @app.route('/login', methods=['POST'])
    def user_login():
        # Increment wms_request metric
        REQUEST.inc()
        # verify if data received is a JSON
        if request.is_json:
            try:
                # Extract json data
                data_dict = request.get_json()
                if data_dict:
                    email = data_dict.get("email")
                    logger.info("Email received:" + email)
                    password = data_dict.get("psw")
                    timestamp_client = data_dict.get("timestamp_client")
                    hash_psw = calculate_hash(password)  # in the DB we have hash of the password
                    try:
                        DBstart_time = time.time_ns()
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:
                            mycursor = mydb.cursor()

                            # check if email already exists in DB
                            mycursor.execute("SELECT email, password FROM users WHERE email=%s and password=%s",
                                             (email, hash_psw))
                            email_row = mycursor.fetchone()
                            DBend_time = time.time_ns()
                            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                            if not email_row:
                                FAILURE.inc()
                                DELTA_TIME.set(time.time_ns() - timestamp_client)
                                return f"Email or password wrong! Retry!", 401
                            else:
                                payload = {
                                    'email': email,
                                    'exp': datetime.datetime.utcnow() + datetime.timedelta(days=3)
                                }
                                token = jwt.encode(payload, hash_psw, algorithm='HS256')
                                DELTA_TIME.set(time.time_ns() - timestamp_client)
                                return f"Login successfully made! JWT Token: {token}", 200

                    except mysql.connector.Error as err:
                        logger.error("Exception raised! -> " + str(err) + "\n")
                        FAILURE.inc()
                        INTERNAL_ERROR.inc()
                        DELTA_TIME.set(time.time_ns() - timestamp_client)
                        return f"Error in connecting to database: {str(err)}", 500

            except Exception as e:
                FAILURE.inc()
                return f"Error in reading data: {str(e)}", 400
        else:
            FAILURE.inc()
            return "Error: the request must be in JSON format", 400

    @app.route('/delete_account', methods=['POST'])
    def delete_account():
        # Increment wms_request metric
        REQUEST.inc()
        # verify if data received is a JSON
        if request.is_json:
            try:
                # Extract json data
                data_dict = request.get_json()
                if data_dict:
                    email = data_dict.get("email")
                    logger.info("Email received:" + email)
                    password = data_dict.get("psw")
                    timestamp_client = data_dict.get("timestamp_client")
                    hash_psw = calculate_hash(password)  # in the DB we have hash of the password
                    try:
                        DBstart_time = time.time_ns()
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:
                            mycursor = mydb.cursor()

                            # check if email already exists in DB
                            mycursor.execute("SELECT id, email, password FROM users WHERE email=%s and password=%s",
                                             (email, hash_psw))
                            email_row = mycursor.fetchone()
                            DBend_time = time.time_ns()
                            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                            if not email_row:
                                FAILURE.inc()
                                DELTA_TIME.set(time.time_ns() - timestamp_client)
                                return f"Email or password wrong! Retry!", 401
                            else:
                                result = delete_UserConstraints_By_UserID(email_row[0])
                                if result != "error":
                                    try:
                                        DBstart_time = time.time_ns()
                                        mycursor.execute("DELETE FROM users WHERE email=%s and password=%s",
                                                         (email, hash_psw))
                                        mydb.commit()
                                        DBend_time = time.time_ns()
                                        QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
                                    except mysql.connector.Error as exception:
                                        logger.error("Exception raised! -> " + str(exception) + "\n")
                                        try:
                                            mydb.rollback()
                                        except Exception as exe:
                                            logger.error(f"Exception raised in rollback: {exe}\n")
                                        # implementing pattern SAGA for distributed transaction
                                        while compensate_user_constraints_elimination(result) != True:
                                            pass
                                    REGISTERED_USERS_COUNT.dec()
                                    DELTA_TIME.set(time.time_ns() - timestamp_client)
                                    return "ACCOUNT DELETED WITH RELATIVE USER_CONSTRAINTS!", 200
                                else:
                                    FAILURE.inc()
                                    INTERNAL_ERROR.inc()
                                    DELTA_TIME.set(time.time_ns() - timestamp_client)
                                    return "Error in grpc communication, account not deleted", 500
                    except mysql.connector.Error as err:
                        logger.error("Exception raised! -> " + str(err) + "\n")
                        try:
                            mydb.rollback()
                        except Exception as exe:
                            logger.error(f"Exception raised in rollback: {exe}\n")
                        FAILURE.inc()
                        INTERNAL_ERROR.inc()
                        DELTA_TIME.set(time.time_ns() - timestamp_client)
                        return f"Error in connecting to database: {str(err)}", 500

            except Exception as e:
                FAILURE.inc()
                return f"Error in reading data: {str(e)}", 400
        else:
            FAILURE.inc()
            return "Error: the request must be in JSON format", 400

    @app.route('/metrics')
    def metrics():
        # Export all the metrics as text for Prometheus
        return Response(generate_latest(REGISTRY), mimetype='text/plain')

    return app


# create Flask application
app = create_app()


def serve_apigateway():
    port = 50053
    hostname = socket.gethostname()
    logger.info(f'Hostname: {hostname} -> server starting on port {str(port)}')
    app.run(host='0.0.0.0', port=port, threaded=True)


if __name__ == '__main__':

    # setting env variables for secrets
    secret_password_path = os.environ.get('PASSWORD')
    with open(secret_password_path, 'r') as file:
        secret_password_value = file.read()
    os.environ['PASSWORD'] = secret_password_value

    logger.info("ENV variables initialization done")

    # Creating table users if not exits
    try:
        DBstart_time = time.time_ns()
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTO_INCREMENT, email VARCHAR(30) UNIQUE NOT NULL, password VARCHAR(64) NOT NULL)")
            mydb.commit()  # to make changes effective
            DBend_time = time.time_ns()
            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
            DBstart_time = time.time_ns()
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS metrics_to_restore (id INTEGER PRIMARY KEY AUTO_INCREMENT, metrics JSON NOT NULL)")
            mydb.commit()
            DBend_time = time.time_ns()
            QUERY_DURATIONS_HISTOGRAM.observe(DBend_time-DBstart_time)
            while check_pending_metrics_to_restore(mycursor) == False:
                pass
    except mysql.connector.Error as err:
        sys.stderr.write("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as e:
            sys.stderr.write(f"Exception raised in rollback: {e}\n")
        sys.exit("User Manager terminating after an error...\n")

    logger.info("Starting notifier serving thread!\n")
    threadNotifier = threading.Thread(target=serve_notifier)
    threadNotifier.daemon = True
    threadNotifier.start()
    logger.info("Starting WMS serving thread!\n")
    threadWMS = threading.Thread(target=serve_wms)
    threadWMS.daemon = True
    threadWMS.start()
    logger.info("Starting API Gateway serving thread!\n")
    threadAPIGateway = threading.Thread(target=serve_apigateway)
    threadAPIGateway.daemon = True
    threadAPIGateway.start()
    # inserted only because created threads are daemon
    while True:
        pass

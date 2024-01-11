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
import json
import socket
from flask import Flask
from flask import request
import hashlib
import datetime
from prometheus_client import Counter, generate_latest, REGISTRY, Gauge
from flask import Response

# definition of the metrics to be exposed
REQUEST = Counter('UM_requests', 'Total number of requests received by um-service')
FAILURE = Counter('UM_failure_requests', 'Total number of requests received by um-service that failed')
INTERNAL_ERROR = Counter('UM_internal_http_error', 'Total number of internal http error in um-service')
RESPONSE_TO_WMS = Counter('UM_RESPONSE_TO_UM', 'Total number of response sent to wms-service')
RESPONSE_TO_NOTIFIER = Counter('UM_RESPONSE_TO_NOTIFIER', 'Total number of response sent to notifier-service')
LOGGED_USERS_COUNT = Gauge('UM_logged_users_count', 'Total number of logged users')
REGISTERED_USERS_COUNT = Gauge('UM_registered_users_count', 'Total number of registered users')
DELTA_TIME = Gauge('UM_response_time_client', 'Latency beetween instant in which client send the API CALL and instant in which user-manager response')

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

    # connection with DB and retrieve user id
    def RequestUserIdViaJWTToken(self, request, context):
        try:
            # extract token information without verifying them: needed in order to retrieve user email
            token_dict = jwt.decode(request.jwt_token, algorithms=['HS256'], options={"verify_signature": False})
            email = token_dict.get("email")
            try:
                with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                             user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                             database=os.environ.get('DATABASE')) as db:
                    cursor = db.cursor()
                    cursor.execute("SELECT id, password FROM users WHERE email= %s", (email,))
                    row = cursor.fetchone()
                    if row:
                        userid = row[0]
                        password = row[1]
                    else:
                        RESPONSE_TO_WMS.inc()
                        return WMS_um_pb2.Reply(user_id=-3)  # token is not valid: email not present
            except mysql.connector.Error as error:
                safe_print_error("Exception raised! -> {0}".format(str(error)))
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
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as db:
                cursor = db.cursor()
                cursor.execute("SELECT email FROM users WHERE id= %s", (str(request.user_id),))
                row = cursor.fetchone()
                if row:
                    email = row[0]
                else:
                    email = "not present anymore"
        except mysql.connector.Error as error:
            safe_print_error("Exception raised! -> {0}".format(str(error)))
            email = "null"
        RESPONSE_TO_NOTIFIER.inc()
        return notifier_um_pb2.Reply(email=email)


def serve_notifier():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_um_pb2_grpc.add_NotifierUmServicer_to_server(NotifierUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    safe_print("Notifier thread server started, listening on " + port + "\n")
    server.wait_for_termination()


def serve_wms():
    port = '50052'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    WMS_um_pb2_grpc.add_WMSUmServicer_to_server(WMSUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    safe_print("WMS thread server started, listening on " + port + "\n")
    server.wait_for_termination()


def calculate_hash(input_string):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(input_string.encode('utf-8'))
    hash_result = sha256_hash.hexdigest()
    return hash_result


def delete_UserConstraints_By_UserID(userId):
    try:
        with grpc.insecure_channel('wms-service:50052') as channel:
            stub = WMS_um_pb2_grpc.WMSUmStub(channel)
            response = stub.RequestDeleteUser_Constraints(WMS_um_pb2.User(user_id=userId))
            code_to_return = response.response_code  # user id < 0 if some error occurred
    except grpc.RpcError as error:
        safe_print_error("gRPC error! -> " + str(error) + "\n")
        code_to_return = -1
    return code_to_return


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
                    safe_print("Email received:" + email)
                    password = data_dict.get("psw")
                    timestamp_client = data_dict.get("timestamp_client")
                    try:
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:
                            mycursor = mydb.cursor()

                            # check if email already exists in DB
                            mycursor.execute("SELECT email FROM users WHERE email=%s", (email,))
                            email_row = mycursor.fetchone()
                            if not email_row:
                                hash_psw = calculate_hash(password)  # we save hash in DB for major privacy for users
                                mycursor.execute("INSERT INTO users (email, password) VALUES (%s,%s)",
                                                 (email, hash_psw))
                                mydb.commit()
                                REGISTERED_USERS_COUNT.inc()
                                DELTA_TIME.set(time.time_ns() - timestamp_client)
                                return "Registration made successfully! Now try to sign in!", 200
                            DELTA_TIME.set(time.time_ns() - timestamp_client)
                            FAILURE.inc()
                            return f"Email already in use! Try to sign in!", 401

                    except mysql.connector.Error as err:
                        safe_print_error("Exception raised! -> " + str(err) + "\n")
                        try:
                            mydb.rollback()
                        except Exception as exe:
                            sys.stderr.write(f"Exception raised in rollback: {exe}\n")
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
                    safe_print("Email received:" + email)
                    password = data_dict.get("psw")
                    timestamp_client = data_dict.get("timestamp_client")
                    hash_psw = calculate_hash(password)  # in the DB we have hash of the password
                    try:
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:
                            mycursor = mydb.cursor()

                            # check if email already exists in DB
                            mycursor.execute("SELECT email, password FROM users WHERE email=%s and password=%s",
                                             (email, hash_psw))
                            email_row = mycursor.fetchone()
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
                                LOGGED_USERS_COUNT.inc()
                                DELTA_TIME.set(time.time_ns() - timestamp_client)
                                return f"Login successfully made! JWT Token: {token}", 200

                    except mysql.connector.Error as err:
                        safe_print_error("Exception raised! -> " + str(err) + "\n")
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
                    safe_print("Email received:" + email)
                    password = data_dict.get("psw")
                    timestamp_client = data_dict.get("timestamp_client")
                    hash_psw = calculate_hash(password)  # in the DB we have hash of the password
                    try:
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:
                            mycursor = mydb.cursor()

                            # check if email already exists in DB
                            mycursor.execute("SELECT id, email, password FROM users WHERE email=%s and password=%s",
                                             (email, hash_psw))
                            email_row = mycursor.fetchone()
                            if not email_row:
                                FAILURE.inc()
                                DELTA_TIME.set(time.time_ns() - timestamp_client)
                                return f"Email or password wrong! Retry!", 401
                            else:
                                if delete_UserConstraints_By_UserID(email_row[0]) != -1:
                                    mycursor.execute("DELETE FROM users WHERE email=%s and password=%s",
                                                     (email, hash_psw))
                                    mydb.commit()
                                    REGISTERED_USERS_COUNT.dec()
                                    LOGGED_USERS_COUNT.dec()
                                    DELTA_TIME.set(time.time_ns() - timestamp_client)
                                    return "ACCOUNT DELETED WITH RELATIVE USER_CONSTRAINTS!", 200
                                else:
                                    FAILURE.inc()
                                    INTERNAL_ERROR.inc()
                                    DELTA_TIME.set(time.time_ns() - timestamp_client)
                                    return "Error in grpc communication, account not deleted", 500
                    except mysql.connector.Error as err:
                        safe_print_error("Exception raised! -> " + str(err) + "\n")
                        try:
                            mydb.rollback()
                        except Exception as exe:
                            sys.stderr.write(f"Exception raised in rollback: {exe}\n")
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
    safe_print(f'Hostname: {hostname} -> server starting on port {str(port)}')
    app.run(host='0.0.0.0', port=port, threaded=True)


if __name__ == '__main__':

    # setting env variables for secrets
    secret_password_path = os.environ.get('PASSWORD')
    with open(secret_password_path, 'r') as file:
        secret_password_value = file.read()
    os.environ['PASSWORD'] = secret_password_value

    # Creating table users if not exits
    try:
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTO_INCREMENT, email VARCHAR(30) UNIQUE NOT NULL, password VARCHAR(64) NOT NULL)")
            mydb.commit()  # to make changes effective
    except mysql.connector.Error as err:
        sys.stderr.write("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as e:
            sys.stderr.write(f"Exception raised in rollback: {e}\n")
        sys.exit("User Manager terminating after an error...\n")

    safe_print("Starting notifier serving thread!\n")
    threadNotifier = threading.Thread(target=serve_notifier)
    threadNotifier.daemon = True
    threadNotifier.start()
    safe_print("Starting WMS serving thread!\n")
    threadWMS = threading.Thread(target=serve_wms)
    threadWMS.daemon = True
    threadWMS.start()
    safe_print("Starting API Gateway serving thread!\n")
    threadAPIGateway = threading.Thread(target=serve_apigateway)
    threadAPIGateway.daemon = True
    threadAPIGateway.start()
    # inserted only because created threads are daemon
    while True:
        pass

import mysql.connector
import os
import sys
import jwt
import socket
from flask import Flask
from flask import request
import hashlib
import datetime
from prometheus_client import Counter, generate_latest, REGISTRY, Gauge, Histogram
from prometheus_api_client import PrometheusConnect, MetricsList, MetricSnapshotDataFrame, MetricRangeDataFrame
from datetime import timedelta
from prometheus_api_client.utils import parse_datetime


def calculate_hash(input_string):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(input_string.encode('utf-8'))
    hash_result = sha256_hash.hexdigest()
    return hash_result


def authenticate(auth_header):
    try:
        jwt_token = auth_header.split(' ')[1]  # Extract token from <Bearer token> string

        token_dict = jwt.decode(jwt_token, algorithms=['HS256'], options={"verify_signature": False})
        email = token_dict.get("email")
        try:
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as db:
                cursor = db.cursor()
                cursor.execute("SELECT id, password FROM users WHERE email= %s", (email,))
                row = cursor.fetchone()
                if row:
                    password = row[1]
                else:
                    return -3  # token is not valid: email not present
        except mysql.connector.Error as error:
            print("Exception raised! -> {0}".format(str(error)))
            return -2
        # verify that password is correct verifying digital signature with secret = password
        jwt.decode(jwt_token, password, algorithms=['HS256'])
        return 1
    except jwt.ExpiredSignatureError:
        return -1  # token is expired
    except jwt.InvalidTokenError:
        return -3  # token is not valid: password incorrect


def verify_metrics_current_violation_status(metrics_list):  # argument is a list of (id, name, min_value, max_value) list

    URL = "http://prometheus-service:9090/"
    prom = PrometheusConnect(url=URL, disable_ssl=True)
    label_config = {'server': 'localhost'}

    violation_count = 0

    status_string_to_be_returned=""

    for metric in metrics_list:
        metric_name = metric[1]
        min_target_value = metric[2]
        max_target_value = metric[3]

        queryResult = prom.get_current_metric_value(metric_name=metric, label_config=label_config)
        print(queryResult)
        actual_string_value = queryResult[0].get("value")[1]
        try:
            actual_value = float(actual_string_value)
            print(f"Metric {metric_name} -> actual value: {actual_value}\n")
            if actual_value < min_target_value or actual_value > max_target_value:
                metric_string = f"Metric name: {metric_name} \n| Actual value: {str(actual_value)} | Min target value: {str(min_target_value)} | Max target value: {str(max_target_value)} | Metric status: VIOLATED!\n\n"
                violation_count = violation_count + 1
                status_string_to_be_returned = status_string_to_be_returned + metric_string
            else:
                metric_string = f"Metric name: {metric_name} \n| Actual value: {str(actual_value)} | Min target value: {str(min_target_value)} | Max target value: {str(max_target_value)} | Metric status: NOT VIOLATED!\n\n"
                status_string_to_be_returned = status_string_to_be_returned + metric_string
        except ValueError:
            print("Metric actual value is not a decimal number!")
            return "ERROR! THERE IS A METRIC WHOSE VALUES IS NOT A DECIMAL NUMBER!"
    status_string_to_be_returned = status_string_to_be_returned + f"Number of violation: {str(violation_count)}\n\n"
    return status_string_to_be_returned


def violation_counter(list_of_metrics, hours):

    URL = "http://prometheus-service:9090/"
    prom = PrometheusConnect(url=URL, disable_ssl=True)
    label_config = {'server': 'localhost'}

    violation_count = 0

    status_string_to_be_returned=""

    for metric in list_of_metrics:
        metric_name = metric[1]
        min_target_value = metric[2]
        max_target_value = metric[3]

        start_time = parse_datetime("1h")
        end_time = parse_datetime("now")

        metric_data = prom.get_metric_range_data(
            metric_name='rt_random',
            label_config=label_config,
            start_time=start_time,
            end_time=end_time,
        )
        print(metric_data)
        actual_string_value = metric_data[0].get("value")[1]
        return "TO BE CONTINUED"

        """"
        try:
            actual_value = float(actual_string_value)
            print(f"Metric {metric_name} -> actual value: {actual_value}\n")
            if actual_value < min_target_value or actual_value > max_target_value:
                metric_string = f"Metric name: {metric_name} \n| Actual value: {str(actual_value)} | Min target value: {str(min_target_value)} | Max target value: {str(max_target_value)} | Metric status: VIOLATED!\n\n"
                violation_count = violation_count + 1
                status_string_to_be_returned = status_string_to_be_returned + metric_string
            else:
                metric_string = f"Metric name: {metric_name} \n| Actual value: {str(actual_value)} | Min target value: {str(min_target_value)} | Max target value: {str(max_target_value)} | Metric status: NOT VIOLATED!\n\n"
                status_string_to_be_returned = status_string_to_be_returned + metric_string
        except ValueError:
            print("Metric actual value is not a decimal number!")
            return "ERROR! THERE IS A METRIC WHOSE VALUES IS NOT A DECIMAL NUMBER!"
    status_string_to_be_returned = status_string_to_be_returned + f"Number of violation: {str(violation_count)}\n\n"
    return status_string_to_be_returned
    """


def create_app():
    app = Flask(__name__)

    @app.route('/login', methods=['POST'])
    def admin_login():
        # verify if data received is a JSON
        if request.is_json:
            try:
                # Extract json data
                data_dict = request.get_json()
                if data_dict:
                    email = data_dict.get("email")
                    print("Email received:" + email)
                    password = data_dict.get("psw")
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
                                return f"Email or password wrong! Retry!", 401
                            else:
                                payload = {
                                    'email': email,
                                    'exp': datetime.datetime.utcnow() + datetime.timedelta(days=3)
                                }
                                token = jwt.encode(payload, hash_psw, algorithm='HS256')
                                return f"Login successfully made! JWT Token: {token}", 200

                    except mysql.connector.Error as err:
                        print("Exception raised! -> " + str(err) + "\n")
                        return f"Error in connecting to database: {str(err)}", 500

            except Exception as e:
                return f"Error in reading data: {str(e)}", 400
        else:
            return "Error: the request must be in JSON format", 400

    @app.route('/SLA_update_metrics', methods=['POST'])
    def update_metrics_handler():
        # Verify if data received is a JSON
        if request.is_json:
            try:
                # Extract json data
                data_dict = request.get_json()
                print("Data received:" + str(data_dict))
                if data_dict:
                    authorization_header = request.headers.get('Authorization')
                    if authorization_header and authorization_header.startswith('Bearer '):
                        result_code = authenticate(authorization_header)
                        if result_code == -1:
                            return 'JWT Token expired: login required!', 401
                        elif result_code == -2:
                            return 'Error in communication with DB in order to authentication: retry!', 500
                        elif result_code == -3:
                            return 'JWT Token is not valid: login required!', 401
                    else:
                        # No token provided in authorization header
                        return 'JWT Token not provided: login required!', 401

                    # admin provides a json containing as key the metric name and as value the max-min target values
                    metric_name_set = data_dict.keys()
                    try:
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:

                            # buffered=True needed because we reuse mycursor after a fetchone()
                            mycursor = mydb.cursor(buffered=True)

                            for metric in metric_name_set:
                                min = data_dict.get(metric)[0]
                                max = data_dict.get(metric)[1]
                                mycursor.execute("SELECT * FROM metrics WHERE metric_name = %s", (metric,))
                                row = mycursor.fetchone()
                                if not row:
                                    print("There is no metric with that name\n")
                                    mycursor.execute(
                                        "INSERT INTO metrics (metric_name, min_target_value, max_target_value) VALUES (%s, %s, %s)",
                                        (metric, min, max))
                                    print("Inserting new metric!\n")
                                else:
                                    mycursor.execute("UPDATE metrics SET min_target_value = %s AND max_target_value = %s WHERE metric_name = %s",
                                                     (min, max, metric))
                                    print("Updating metric table!\n")
                            mydb.commit()
                            return "Metric table updated correctly!", 200

                    except mysql.connector.Error as err:
                        print("Exception raised! -> " + str(err) + "\n")
                        try:
                            mydb.rollback()
                        except Exception as exe:
                            print(f"Exception raised in rollback: {exe}\n")
                        return f"Error in connecting to database: {str(err)}", 500

            except Exception as e:
                return f"Error in reading data: {str(e)}", 400
        else:
            return "Error: the request must be in JSON format", 400

    @app.route('/SLA_metrics_status')
    def status_handler():
        authorization_header = request.headers.get('Authorization')
        if authorization_header and authorization_header.startswith('Bearer '):
            result_code = authenticate(authorization_header)
            if result_code == -1:
                return 'JWT Token expired: login required!', 401
            elif result_code == -2:
                return 'Error in communication with DB in order to authentication: retry!', 500
            elif result_code == -3:
                return 'JWT Token is not valid: login required!', 401
        else:
            # No token provided in authorization header
            return 'JWT Token not provided: login required!', 401

        try:
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as mydb:

                # buffered=True needed because we reuse after a fetchone
                mycursor = mydb.cursor(buffered=True)

                # retrieve all the metrics
                mycursor.execute("SELECT * FROM metrics")
                rows = mycursor.fetchall()
                if not rows:
                    return "There is no metrics that have been indicated!", 200
                else:
                    result = verify_metrics_current_violation_status(rows)
                    return f"STATUS OF METRICS: \n\n {result}", 200

        except mysql.connector.Error as err:
            print("Exception raised! -> " + str(err) + "\n")
            try:
                mydb.rollback()
            except Exception as exe:
                print(f"Exception raised in rollback: {exe}\n")
            return f"Error in connecting to database: {str(err)}", 500

    @app.route('/SLA_metrics_violations')
    def status_handler():
        authorization_header = request.headers.get('Authorization')
        if authorization_header and authorization_header.startswith('Bearer '):
            result_code = authenticate(authorization_header)
            if result_code == -1:
                return 'JWT Token expired: login required!', 401
            elif result_code == -2:
                return 'Error in communication with DB in order to authentication: retry!', 500
            elif result_code == -3:
                return 'JWT Token is not valid: login required!', 401
        else:
            # No token provided in authorization header
            return 'JWT Token not provided: login required!', 401

        try:
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                         user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                         database=os.environ.get('DATABASE')) as mydb:

                # buffered=True needed because we reuse after a fetchone
                mycursor = mydb.cursor(buffered=True)

                # retrieve all the metrics
                mycursor.execute("SELECT * FROM metrics")
                rows = mycursor.fetchall()
                if not rows:
                    return "There is no metrics that have been indicated!", 200
                else:
                    result_1hour = violation_counter(rows, 1)
                    result_3hour = violation_counter(rows, 3)
                    result_6hour = violation_counter(rows, 6)
                    return f"METRICS VIOLATED: \n\n{result_1hour}\n\n{result_3hour}\n\n{result_6hour}", 200

        except mysql.connector.Error as err:
            print("Exception raised! -> " + str(err) + "\n")
            try:
                mydb.rollback()
            except Exception as exe:
                print(f"Exception raised in rollback: {exe}\n")
            return f"Error in connecting to database: {str(err)}", 500


    return app


# create Flask application
app = create_app()


if __name__ == '__main__':

    # setting env variables for secrets
    secret_password_path = os.environ.get('PASSWORD')
    with open(secret_password_path, 'r') as file:
        secret_password_value = file.read()
    os.environ['PASSWORD'] = secret_password_value

    print("ENV variables initialization done")

    # Creating table users if not exits
    try:
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                     database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS metrics (id INTEGER PRIMARY KEY AUTO_INCREMENT, metric_name VARCHAR(100) UNIQUE NOT NULL, min_target_value DOUBLE NOT NULL, max_target_value DOUBLE NOT NULL)")
            mycursor.execute(
                "CREATE TABLE IF NOT EXISTS admins (id INTEGER PRIMARY KEY AUTO_INCREMENT, email VARCHAR(30) UNIQUE NOT NULL, password VARCHAR(64) NOT NULL)")
            mydb.commit()  # to make changes effective
    except mysql.connector.Error as err:
        sys.stderr.write("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as e:
            sys.stderr.write(f"Exception raised in rollback: {e}\n")
        sys.exit("User Manager terminating after an error...\n")

    port = 50055
    hostname = socket.gethostname()
    print(f'Hostname: {hostname} -> server starting on port {str(port)}')
    app.run(host='0.0.0.0', port=port, threaded=True)

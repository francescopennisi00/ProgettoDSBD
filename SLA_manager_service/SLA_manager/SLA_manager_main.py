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
import logging
from statsmodels.tsa.holtwinters import SimpleExpSmoothing,ExponentialSmoothing
import matplotlib.pyplot as plt
from io import BytesIO
import warnings
#from statsmodels.tools.sm_exceptions import ConvergenceWarning
#warnings.simplefilter('ignore', ConvergenceWarning)

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
                cursor.execute("SELECT id, password FROM admins WHERE email= %s", (email,))
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


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def verify_metrics_current_violation_status(
        metrics_list):  # argument is a list of (id, name, min_value, max_value) list

    URL = "http://prometheus-service:9090/"
    prom = PrometheusConnect(url=URL, disable_ssl=True)

    violation_count = 0

    status_string_to_be_returned = ""
    logger.info("Metric list" + str(metrics_list))
    for metric in metrics_list:
        logger.info("metric" + str(metric))
        metric_name = metric[1]
        logger.info("metric name" + metric_name)
        min_target_value = metric[2]
        max_target_value = metric[3]

        queryResult = prom.get_current_metric_value(metric_name=metric_name)
        logger.info(str(queryResult))
        actual_string_value = queryResult[0].get("value")[1]
        try:
            actual_value = float(actual_string_value)
            print(f"Metric {metric_name} -> actual value: {actual_value}\n")
            if actual_value < min_target_value or actual_value > max_target_value:
                metric_string = f"Metric name: {metric_name}" + "<br>" + f"Actual value: {str(actual_value)} | Min target value: {str(min_target_value)} | Max target value: {str(max_target_value)} | Metric status: VIOLATED!" + "<br><br>"
                violation_count = violation_count + 1
                status_string_to_be_returned = status_string_to_be_returned + metric_string
            else:
                metric_string = f"Metric name: {metric_name}" + "<br>" + f"Actual value: {str(actual_value)} | Min target value: {str(min_target_value)} | Max target value: {str(max_target_value)} | Metric status: NOT VIOLATED!" + "<br><br>"
                status_string_to_be_returned = status_string_to_be_returned + metric_string
        except ValueError:
            print("Metric actual value is not a decimal number!")
            return "ERROR! THERE IS A METRIC WHOSE VALUES IS NOT A DECIMAL NUMBER!"
    status_string_to_be_returned = status_string_to_be_returned + f"Number of violation: {str(violation_count)} " + "<br><br>"
    return status_string_to_be_returned


def violation_counter(list_of_metrics, hours):
    URL = "http://prometheus-service:9090/"
    prom = PrometheusConnect(url=URL, disable_ssl=True)

    status_string_to_be_returned = f"VIOLATIONS IN THE LAST {hours} HOURS <br><br>"

    for metric in list_of_metrics:
        metric_string = ""
        violation_count = 0
        metric_name = metric[1]
        min_target_value = metric[2]
        max_target_value = metric[3]

        start_time = parse_datetime(str(hours) + "h")
        end_time = parse_datetime("now")

        metric_data = prom.get_metric_range_data(
            metric_name=metric_name,
            start_time=start_time,
            end_time=end_time,
        )
        logger.info("METRIC " + str(metric_data))
        try:
            for element in metric_data[0].get('values'):
                actual_string_value = element[1]
                actual_value = float(actual_string_value)
                print(f"Metric {metric_name} -> actual value: {actual_value}\n")
                if actual_value < min_target_value or actual_value > max_target_value:
                    violation_count = violation_count + 1
            if violation_count > 0:
                metric_string = f"Metric name: {metric_name} Violations number:{violation_count} <br>"
        except ValueError:
            print("Metric actual value is not a decimal number!")
            return "ERROR! THERE IS A METRIC WHOSE VALUES IS NOT A DECIMAL NUMBER!"
        status_string_to_be_returned = status_string_to_be_returned + metric_string
    return status_string_to_be_returned


def metrics_forecasting(metric, minutes):
    URL = "http://prometheus-service:9090/"
    prom = PrometheusConnect(url=URL, disable_ssl=True)

    metric_name = metric[1]
    min_target_value = metric[2]
    max_target_value = metric[3]
    violation_count = 0
    start_time = parse_datetime("3h")
    end_time = parse_datetime("now")
    status_string_to_be_returned = ""
    metric_string = ""
    metric_data = prom.get_metric_range_data(
        metric_name=metric_name,
        start_time=start_time,
        end_time=end_time,
    )
    metric_df = MetricRangeDataFrame(metric_data)
    logger.info("METRIC_DF\n" + str(metric_df))
    value_list = metric_df['value']
    logger.info("NUMBER OF NON VALUE " + str(value_list.isna().sum()))
    logger.info("VALUE_LIST INDEX\n " + str(value_list.index))
    logger.info("VALUE_LIST\n " + str(value_list))
    tsr = value_list.resample(rule='30s').mean()
    logger.info("TSR NUMBER OF NON VALUE " + str(tsr.isna().sum()))
    tsr = tsr.interpolate()
    logger.info("TSR INTERPOLATE NUMBER OF NON VALUE " + str(tsr.isna().sum()))
    logger.info("TSR\n " + str(tsr))
    logger.info("TSR_INDEX\n " + str(tsr.index))
    # Split training and test data (80/20% 0r 90/10)
    len_dataframe = len(metric_df)
    end = 0.8 * len_dataframe
    end_index = round(end)
    train_data = tsr.iloc[:end_index]
    logger.info("TRAIN DATA NUMBER OF NON VALUE " + str(train_data.isna().sum()))
    test_data = tsr.iloc[end_index:]
    logger.info("TEST DATA NUMBER OF NON VALUE " + str(test_data.isna().sum()))
    logger.info("TEST DATA\n " + str(test_data))
    tsmodel = ExponentialSmoothing(train_data, trend='add', seasonal="add", seasonal_periods=15).fit()
    # forecast (check 320)
    try:
        minutes_int = int(minutes)  # required because minutes GET parameter is a string
    except ValueError:
        return "parameter_error"
    steps = minutes_int*2  # each step takes 30 seconds, 2 step each minute
    prediction = tsmodel.forecast(steps)
    logger.info("PREDICTION\n " + str(prediction))
    logger.info("\nTYPE PREDICTION\n " + str(type(prediction)))
    try:
        for element in prediction:
            logger.info("\nELEMENT OF PREDICTION " + str(element))
            actual_value = float(element)
            print(f"Metric {metric_name} -> actual value: {actual_value}\n")
            if actual_value < min_target_value or actual_value > max_target_value:
                violation_count = violation_count + 1
        if violation_count > 0:
            metric_string = f"Metric name: {metric_name} Violations number:{violation_count} <br>"
    except ValueError:
        print("Metric actual value is not a decimal number!")
        return "ERROR! THERE IS A METRIC WHOSE VALUES IS NOT A DECIMAL NUMBER!"
    status_string_to_be_returned = status_string_to_be_returned + metric_string
    return train_data, test_data, prediction



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
                            mycursor.execute("SELECT email, password FROM admins WHERE email=%s and password=%s",
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
                                    mycursor.execute(
                                        "UPDATE metrics SET min_target_value = %s, max_target_value = %s WHERE metric_name = %s",
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

    @app.route('/SLA_delete_metrics', methods=['POST'])
    def delete_metrics_handler():
        # Verify if data received is a JSON (it should be like {"metrics": [metric_name1, metric_name2, ... ]})
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

                    metrics_names_list = data_dict.get("metrics")
                    try:
                        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'),
                                                     user=os.environ.get('USER'), password=os.environ.get('PASSWORD'),
                                                     database=os.environ.get('DATABASE')) as mydb:

                            # buffered = True is required because we reuse db cursor after a fetchone()
                            mycursor = mydb.cursor(buffered=True)
                            string_to_be_returned = ""
                            found = 0
                            for metric in metrics_names_list:
                                mycursor.execute("SELECT * FROM metrics WHERE metric_name = %s", (metric,))
                                metric_row = mycursor.fetchone()
                                if metric_row:
                                    mycursor.execute("DELETE FROM metrics WHERE metric_name = %s", (metric,))
                                    found = 1
                                else:
                                    string_to_be_returned = string_to_be_returned + f"The metric {metric} is not present in the database! Therefore, it cannot be deleted!<br>"
                            mydb.commit()
                            if string_to_be_returned == "":
                                string_to_be_returned = "Metrics deleted correctly!"
                            else:
                                if found == 1:
                                    string_to_be_returned = string_to_be_returned + "Other metrics deleted correctly!"

                            return string_to_be_returned, 200

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

                mycursor = mydb.cursor()

                # retrieve all the metrics
                mycursor.execute("SELECT * FROM metrics")
                rows = mycursor.fetchall()
                if not rows:
                    return "There is no metrics that have been indicated!", 200
                else:
                    result = verify_metrics_current_violation_status(rows)
                    return f"STATUS OF METRICS: <br><br> {result}", 200

        except mysql.connector.Error as err:
            print("Exception raised! -> " + str(err) + "\n")
            try:
                mydb.rollback()
            except Exception as exe:
                print(f"Exception raised in rollback: {exe}\n")
            return f"Error in connecting to database: {str(err)}", 500

    @app.route('/SLA_metrics_violations')
    def metrics_violations_handler():
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

                mycursor = mydb.cursor()

                # retrieve all the metrics
                mycursor.execute("SELECT * FROM metrics")
                rows = mycursor.fetchall()
                if not rows:
                    return "There is no metrics that have been indicated!", 200
                else:
                    result_1hour = violation_counter(rows, 1)
                    result_3hour = violation_counter(rows, 3)
                    result_6hour = violation_counter(rows, 6)
                    return f"METRICS VIOLATED: <br><br> {result_1hour} <br><br> {result_3hour} <br><br> {result_6hour}", 200

        except mysql.connector.Error as err:
            print("Exception raised! -> " + str(err) + "\n")
            try:
                mydb.rollback()
            except Exception as exe:
                print(f"Exception raised in rollback: {exe}\n")
            return f"Error in connecting to database: {str(err)}", 500

    @app.route('/SLA_forecasting_violations')
    def forecasting_handler():
        minutes = request.args.get('minutes')
        metric_name = request.args.get('metric_name')
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

                mycursor = mydb.cursor()

                # retrieve all the metrics
                mycursor.execute("SELECT * FROM metrics WHERE metric_name = %s", (metric_name,))
                row = mycursor.fetchone()
                if not row:
                    return f"There is no {metric_name} metrics!", 200
                else:
                    result = metrics_forecasting(row, minutes)
                    if result == "parameter_error":
                        return f"Parameter error: minutes must be an integer ", 400
                    else:
                        train_data = result[0]
                        test_data = result[1]
                        prediction = result[2]
                        plt.figure(figsize=(24, 10))
                        plt.ylabel('Values', fontsize=14)
                        plt.xlabel('Time', fontsize=14)
                        plt.title('Values over time', fontsize=16)
                        plt.plot(train_data, "-", label='train')
                        plt.plot(test_data, "-", label='real')
                        plt.plot(prediction, "--", label='pred')
                        plt.legend(title='Series')
                        buffer = BytesIO()
                        plt.savefig(buffer, format='png')
                        buffer.seek(0)
                        plt.close()
                        return app.response_class(buffer.getvalue(), mimetype='image/png'),200


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
    secret_password_path = os.environ.get('EMAIL')
    with open(secret_password_path, 'r') as file:
        secret_password_value = file.read()
    os.environ['EMAIL'] = secret_password_value
    secret_password_path = os.environ.get('ADMIN_PASSWORD')
    with open(secret_password_path, 'r') as file:
        secret_password_value = file.read()
    os.environ['ADMIN_PASSWORD'] = secret_password_value

    print("ENV variables initialization done")

    # Creating table admins if not exits
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
            mycursor.execute("SELECT * FROM admins")
            result = mycursor.fetchall()
            if not result:
                psw = calculate_hash(os.environ.get("ADMIN_PASSWORD"))
                mycursor.execute("INSERT INTO admins(email, password) VALUES(%s,%s)",(os.environ.get("EMAIL"), psw))
                mydb.commit()
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

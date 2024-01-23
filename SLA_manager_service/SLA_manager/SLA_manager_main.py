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


def create_app():
    app = Flask(__name__)

    @app.route('/login', methods=['POST'])
    def user_login():
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
    def update_rules_handler():
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

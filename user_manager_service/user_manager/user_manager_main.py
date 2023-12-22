import threading
from concurrent import futures
import grpc
import notifier_um_pb2
import notifier_um_pb2_grpc
import mysql.connector
import os
import sys


class NotifierUm(notifier_um_pb2_grpc.NotifierUmServicer):

    # connection with DB and retrieve email
    def RequestEmail(self, request, context):
        try:
            with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as db:
                cursor = db.cursor()
                cursor.execute("SELECT email FROM users WHERE id= %s", (str(request.user_id),))
                row = cursor.fetchone()
                email = row[0]
        except mysql.connector.Error as error:
            sys.stderr.write("Exception raised! -> {0}".format(str(error)))
            email = "null"
        return notifier_um_pb2.Reply(email=email)


def serve_notifier():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_um_pb2_grpc.add_NotifierUmServicer_to_server(NotifierUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("Notifier thread server started, listening on " + port + "\n")
    server.wait_for_termination()


def serve_wms():
    port = '50052'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_um_pb2_grpc.add_NotifierUmServicer_to_server(NotifierUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("WMS thread server started, listening on " + port + "\n")
    server.wait_for_termination()


def serve_apigateway():
    port = '50053'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_um_pb2_grpc.add_NotifierUmServicer_to_server(NotifierUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("API Gateway server started, listening on " + port + "\n")
    server.wait_for_termination()


if __name__ == '__main__':

    # setting env variables for secrets
    secret_password_path = os.environ.get('PASSWORD')
    with open(secret_password_path, 'r') as file:
        secret_password_value = file.read()
    os.environ['PASSWORD'] = secret_password_value

    # Creating table users if not exits
    try:
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
            mycursor = mydb.cursor()
            mycursor.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTO_INCREMENT, email VARCHAR(30) UNIQUE NOT NULL, password VARCHAR(30) NOT NULL)")  #TODO: to insert token JWT
            mycursor.execute("INSERT INTO users (email, password) VALUES (%s,%s)", ("franciccio.pennisi@gmail.com", "prova"))
            mycursor.execute("INSERT INTO users (email, password) VALUES (%s,%s)", ("ale10geno@gmail.com", "prova"))
        mydb.commit()  # to make changes effective
    except mysql.connector.Error as err:
        sys.stderr.write("Exception raised! -> " + str(err) + "\n")
        try:
            mydb.rollback()
        except Exception as e:
            sys.stderr.write(f"Exception raised in rollback: {e}\n")
        sys.exit("User Manager terminating after an error...\n")

    print("Starting notifier serving thread!\n")
    threadNotifier = threading.Thread(target=serve_notifier())
    print("Starting WMS serving thread!\n")
    threadWMS = threading.Thread(target=serve_wms())
    print("Starting API Gateway serving thread!\n")
    threadAPIGateway = threading.Thread(target=serve_apigateway())

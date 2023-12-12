import threading
from concurrent import futures
import grpc
import notifier_um_pb2
import notifier_um_pb2_grpc
import mysql.connector
import os


class NotifierUm(notifier_um_pb2_grpc.NotifierUmServicer):

    def RequestEmail(self, request, context):
        # connection with DB and retrieve email
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
            try:
                mycursor = mydb.cursor()
                mycursor.execute("SELECT email FROM users WHERE id= %s", (str(request.user_id),))
                row = mycursor.fetchone()
                email = row[0]
                return notifier_um_pb2.Reply(email=email)
            except mysql.connector.Error as err:
                print("Exception raised!\n" + str(err))
                return notifier_um_pb2.Reply(email="null")


def serve_notifier():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_um_pb2_grpc.add_NotifierUmServicer_to_server(NotifierUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("Notifier thread server started, listening on " + port)
    server.wait_for_termination()


def serve_wms():
    port = '50052'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_um_pb2_grpc.add_NotifierUmServicer_to_server(NotifierUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("WMS thread server started, listening on " + port)
    server.wait_for_termination()


def serve_apigateway():
    port = '50053'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_um_pb2_grpc.add_NotifierUmServicer_to_server(NotifierUm(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("API Gateway server started, listening on " + port)
    server.wait_for_termination()


if __name__ == '__main__':
    # Creating table users if not exits
    with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port=os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
        try:
            mycursor = mydb.cursor()
            mycursor.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTO_INCREMENT, email VARCHAR(30) UNIQUE NOT NULL, password VARCHAR(30) NOT NULL)")  #TODO: to insert token JWT
            mydb.commit()  # to make changes effective
        except mysql.connector.Error as err:
            mydb.rollback()
            print("Exception raised!\n" + str(err))
            raise SystemExit
    print("Starting notifier serving thread !")
    threadNotifier = threading.Thread(target=serve_notifier())
    print("Starting WMS serving thread!")
    threadWMS = threading.Thread(target=serve_wms())
    print("Starting API Gateway serving thread!")
    threadAPIGateway = threading.Thread(target=serve_apigateway())

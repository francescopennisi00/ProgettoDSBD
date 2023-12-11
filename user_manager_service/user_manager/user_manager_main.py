import threading
from concurrent import futures
import grpc
import notifier_ue_pb2
import notifier_ue_pb2_grpc
import mysql.connector
import os



class NotifierUe(notifier_ue_pb2_grpc.NotifierUeServicer):

    def RequestEmail(self, request, context):
        #connection with DB and retrieve email
        with mysql.connector.connect(host=os.environ.get('HOSTNAME'), port= os.environ.get('PORT'), user=os.environ.get('USER'), password=os.environ.get('PASSWORD'), database=os.environ.get('DATABASE')) as mydb:
            try:
                mycursor = mydb.cursor()
                mycursor.execute("SELECT email FROM users WHERE id= %s", (str(request.user_id),))
                row = mycursor.fetchone()
                email = row[0]
                return notifier_ue_pb2.Reply(email=email)
            except mysql.connector.Error as err:
                print("Exception raised!\n" + str(err))
                return notifier_ue_pb2.Reply(email="null")

def serveNotifier():
    port = '50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_ue_pb2_grpc.add_NotifierUeServicer_to_server(NotifierUe(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("Notifier thread server started, listening on " + port)
    server.wait_for_termination()

def serveWMS():
    port = '50052'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_ue_pb2_grpc.add_NotifierUeServicer_to_server(NotifierUe(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("WMS thread server started, listening on " + port)
    server.wait_for_termination()

def serveAPIGateway():
    port = '50053'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    notifier_ue_pb2_grpc.add_NotifierUeServicer_to_server(NotifierUe(), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    print("API Gateway server started, listening on " + port)
    server.wait_for_termination()


if __name__ == '__main__':
    print("Starting notifier serving thread !")
    threadNotifier = threading.Thread(target=serveNotifier())
    print("Starting WMS serving thread!")
    threadWMS = threading.Thread(target=serveWMS())
    print("Starting API Gateway serving thread!")
    threadAPIGateway = threading.Thread(target=serveAPIGateway())

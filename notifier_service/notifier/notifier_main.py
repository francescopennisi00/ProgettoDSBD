from confluent_kafka import Consumer
import json
import grpc
import notifier_ue_pb2
import notifier_ue_pb2_grpc
from email.message import EmailMessage
import ssl
import smtplib



def commit_completed(err):
    if err:
        print(str(err))
    else:
        print("Notification fetched and stored in DB in order to be sent!")

c = Consumer({'bootstrap.servers': 'kafka-1:29092',
              'group.id': 'group1',
              'enable.auto.commit': 'false',
              'auto.offset.reset': 'none',  #TODO: ragionarci su se conviene none o latest
              'on_commit': commit_completed
              })
c.subscribe(['event_to_be_notified'])
try:
    while True:
        msg = c.poll(timeout=5.0)
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:

            # Check for Kafka message
            record_key = msg.key()
            print(record_key)
            record_value = msg.value()
            print(record_value)
            data = json.loads(record_value)
            userId = data['user_id']
            location = data['location_id']
            violated_rules = data['violated_rules']

            #connection with DB and store event to be notified


            #make commit
            c.commit(asynchronous=True)

            #communication with user management in order to get user email
            with grpc.insecure_channel('user_management:50051') as channel:
                stub = notifier_ue_pb2_grpc.NotifierUeStub(channel)
                response = stub.RequestEmail(notifier_ue_pb2.Request(user_id=userId))
            print(response.email)
            email = response.email

            #send notification by email
            email_sender = "noreplydsbd@gmail.com"
            email_password = "ifph uxrh kjaf ylkt"  #TODO: forse da nascondere
            email_receiver = email
            subject = "Alert notification!"
            body = " messaggio di prova"   #TODO: da modificare con l'elenco delle rules violate
            em=EmailMessage()
            em['From'] = email_sender
            em['To'] = email_receiver
            em['Subject'] = subject
            em.set_content(body)
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
                smtp.login(email_sender,email_password)
                smtp.sendmail(email_sender,email_receiver, em.as_string())
                smtp.close()

            #connection with DB and update the entry of the notification sent

except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    c.close()

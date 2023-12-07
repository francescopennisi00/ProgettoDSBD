from confluent_kafka import Consumer
import json
import grpc
import notifier_ue_pb2
import notifier_ue_pb2_grpc



def commit_completed(err):
    if err:
        print(str(err))
    else:
        print("Notification sent!")

c = Consumer({'bootstrap.servers': 'kafka_1:29092',
              'group.id': 'group1',
              'enable.auto.commit': 'false',
              'auto.offset.reset': 'none',  # 'auto.offset.reset=earliest' to start reading from the beginning - [latest, earliest, none]
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
            #communication with user management in order to get user email
            with grpc.insecure_channel('user_management:50051') as channel:
                stub = notifier_ue_pb2_grpc.NotifierUeStub(channel)
                response = stub.RequestEmail(notifier_ue_pb2.Request(user_id=userId))
            print(response.res)
            email = "test" #fetched by response
            #send notification by email

            # if msg_count % MIN_COMMIT_COUNT == 0:
            c.commit(asynchronous=True)
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    c.close()

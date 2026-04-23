from confluent_kafka import Consumer
import json
consumer_config = {'bootstrap.servers': 'localhost:9092',
                   'group.id':'order-tracker',
                    'auto.offset.reset': 'earliest'}

consumer = Consumer(consumer_config)


consumer.subscribe(['order1'])
print("consumer is running and listening for orders")

try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print('err: ', msg.error())
                continue

            val = json.loads(msg.value().decode('utf-8'))
            print("msg: ", val['user'], val['product_id'])
except KeyboardInterrupt:
        print("closing connection")

finally:
        consumer.close()

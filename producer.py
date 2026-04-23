#food delivery
from confluent_kafka import Producer
import uuid, json
producer_config ={'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)

order = {
    'order_id': str(uuid.uuid4()),
    'product_id': '2gfgds34',
    'user': 'Siddh23452es'
}
#convert to kafka compatible format
value = json.dumps(order)

def delivery_fn(err, msg):
    if err:
        print('Message delivery failed: {}'.format(err))
    print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
    print("this", msg.value().decode('utf-8'))


producer.produce(topic='order1', value=value ,callback=delivery_fn)

# if program crashses it will take care of remaining events
producer.flush()
# (.venv) siddheshkukade@Siddheshs-MacBook-Air-2 StreamStore % docker exec -it kafka-sid kafka-topics --list --bootstrap-server localhost:9092
# OP: order1

# (.venv) siddheshkukade@Siddheshs-MacBook-Air-2 StreamStore % docker exec -it kafka-sid kafka-topics --bootstrap-server localhost:9092 --describe --topic order1
# Topic: order1   TopicId: QrVMK26CSMGJR72cC7MGWQ PartitionCount: 1       ReplicationFactor: 1    Configs:
#         Topic: order1   Partition: 0    Leader: 1       Replicas: 1     Isr: 1  Elr:    LastKnownElr:



# .venv) siddheshkukade@Siddheshs-MacBook-Air-2 StreamStore % docker exec -it kafka-sid kafka-console-consumer --bootstrap-server localhost:9092 --topic order1 --from-beginning
# {"order_id": "e906dcb0-cdc6-4361-934f-2d689956c66e", "product_id": "234", "user": "Siddhes"}
# {"order_id": "1ae4b423-4738-47f6-b5bf-9621eca09d6e", "product_id": "234", "user": "Siddhes"}
# {"order_id": "89eaac4e-a8b3-4004-9e76-9a21223a7ee9", "product_id": "234", "user": "Siddhes"}
# {"order_id": "07f85f8c-766a-4664-8c0d-9ab839f620d6", "product_id": "234", "user": "Siddhes"}

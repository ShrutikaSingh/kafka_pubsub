import time
from confluent_kafka import Consumer, KafkaException, KafkaError

# Wait for Kafka to be ready
time.sleep(10)

conf = {'bootstrap.servers': "kafka:9092",
        'group.id': "test-group",
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)
consumer.subscribe(['test-topic'])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f"Consumed message: {msg.key().decode('utf-8')}: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

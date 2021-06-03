"""
A simple kafka agent inc harge of reading from a topic and writing to a topic (Includes creating topics when needed)
"""
from aiokafka import AIOKafkaProducer as Producer
from aiokafka import AIOKafkaConsumer as Consumer


class KafkaAgent:
    def __init__(self, loop, host, ):
        self.producer: Producer = Producer(producer_conf)
        if consumer_conf:
            self.consumer = Consumer(consumer_conf)

    async def write(self, topic, key, value, **kwargs):
        await self.producer.produce(topic, key=key, value=value, **kwargs)


if __name__ == '__main__':
    import socket
    conf = {'bootstrap.servers': "host1:9092,host2:9092",
            'client.id': socket.gethostname()}
    agent = KafkaAgent(conf)
    agent.write("Test", key="test-key", value="testvalue")
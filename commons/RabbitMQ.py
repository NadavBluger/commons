"""
A a-synchros RabbitMQ agent meant to handle both reading and writing to the rabbitMQ

Example for a configuration in the JSON format
{
    connection_url:'amqp://amdin:admin@127.0.0.1/'
    output_exchanges:
        [
            ('Reader_out',{type:fanout, durable: True #can contain any argument that the exchange declaration will take}),
            ('Reader_out2',{type:fanout, durable: False}),
            ('Reader_out3',{})
        ],
    input_exchanges:['Reader_in','Reader_in2'],
    input_queue:('queueueue',{durable = True, max-limit=1000})
}
"""
import aio_pika
import configurations
if __name__ == '__main__':
    import asyncio
    from queue import Queue


class OutputExchange:
    def __init__(self, name=None, details=None):
        self.name = name
        self.details = details


class InputQueue:
    def __init__(self, name=None, details=None):
        self.name = name
        self.details = details


class RabbitMQAgent:
    def __init__(self, config: configurations.Configuration, loop):
        self.connection_url = config.connection_url
        self.output_exchanges = [OutputExchange(name, details) for name, details in config.output_exchanges]
        self.input_exchanges_names = config.input_exchanges
        self.input_queue = InputQueue(*config.input_queue)
        self.loop = loop

    @staticmethod
    def manual_init(connection_url, output_exchanges: [tuple], input_exchanges: [tuple], input_queue, loop):
        """Build a rabbitMQ agent from arguments given manualy rather than packeged as configuration object"""
        config = configurations.Configuration(["connection_url", "output_exchanges", "input_exchanges", "input_queue"])
        config.connection_url = connection_url
        config.output_exchanges = output_exchanges
        config.input_exchanges = input_exchanges
        config.input_queue = input_queue
        return RabbitMQAgent(config, loop)

    async def connect(self):
        return await aio_pika.connect_robust(self.connection_url, loop=self.loop)

    async def write(self, message, routing_key = "/"):
        """
        Write a single message to all the agents output exchanges
        :param message: The contents of the message to be written
        :param routing_key: The routing key to route to the message
        :return:
        """
        async with await self.connect() as con:
            channel = await con.channel()
            for exchange in self.output_exchanges:
                try:
                    exchange = await channel.declare_exchange(exchange.name, **exchange.details)
                finally:
                    await exchange.publish(message, routing_key)

    async def read(self):
        """
        Read a single message from the agents queue and return it
        :return:
        """
        async with await self.connect() as con:
            channel = await con.channel()
            try:
                queue = channel.declare_queue(self.input_queue.name, **self.input_queue.details)
            finally:
                for exchange in self.input_exchanges_names:
                    queue.bind(exchange)
            return queue.consume()


if __name__ == '__main__':
    configuration = {
        "connection_url": "",
        "output_exchanges": [],
        "input_exchanges": [],
        "input_queue": ()

    }
    confi = configurations.JSONConfiguration([])
    confi.parse_json(configuration)
    loop = asyncio.get_event_loop()
    agent = RabbitMQAgent(confi, loop)

"""
A a-synchronise RabbitMQ agent meant to handle both reading and writing to the rabbitMQ

Example for a configuration in the JSON format
{
    'credentials':'username:password',
    'host': '0.0.0.0',
    'output_exchanges':
        [
            ['Reader_out',{'type':'fanout', 'durable': true #can contain any argument that the exchange declaration will take}],
            ['Reader_out2',{'type':'fanout', 'durable': false}],
            ['Reader_out3',{}]
        ],
    'input_exchanges':['Reader_in','Reader_in2'],
    'input_queue':['queueueue',{'durable':true, 'max-limit':1000}]
}
"""
import aio_pika
import configurations


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
        self.username, self.password = config.credentials.split(":")
        self.host = config.host
        self.output_exchanges = [OutputExchange(name, details) for name, details in config.output_exchanges]
        self.input_exchanges_names = config.input_exchanges
        self.input_queue = InputQueue(*config.input_queue)
        self.loop = loop

    @staticmethod
    def manual_init(credentials, host, output_exchanges: [tuple], input_exchanges: [tuple], input_queue, loop):
        """Build a rabbitMQ agent from arguments given manually rather than packaged as configuration object"""
        config = configurations.Configuration(["connection_url", "output_exchanges", "input_exchanges", "input_queue"])
        config.credentials = credentials
        config.host = host
        config.output_exchanges = output_exchanges
        config.input_exchanges = input_exchanges
        config.input_queue = input_queue
        return RabbitMQAgent(config, loop)

    async def connect(self):
        return await aio_pika.connect_robust(login=self.username,
                                             password=self.password,
                                             loop=self.loop,
                                             host=self.host)

    async def write(self, message, routing_key="/", connection=None):
        """
        Write a single message to all the agents output exchanges
        :param message: The contents of the message to be written
        :param routing_key: The routing key to route to the message
        :param connection: a connection to the queue manager
        :return:
        """
        if not connection:
            con = await self.connect()
        else:
            con = connection
        async with con:
            channel = await con.channel()
            for exchange in self.output_exchanges:
                try:
                    exchange = await channel.declare_exchange(exchange.name, **exchange.details)
                finally:
                    await exchange.publish(aio_pika.Message(message.encode()), routing_key=routing_key)

    async def read(self, connection=None):
        """
        Read a single message from the agents queue and return it
        :param connection: a connection to the queue manager
        :return:
        """
        if not connection:
            con = await self.connect()
        else:
            con = connection
        async with con:
            channel = await con.channel()
            try:
                queue = await channel.declare_queue(self.input_queue.name, **self.input_queue.details)
            finally:
                for exchange in self.input_exchanges_names:
                    await queue.bind(exchange)
            return await queue.get(no_ack=True)

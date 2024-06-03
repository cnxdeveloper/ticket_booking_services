from kafka import KafkaProducer
from kafka.errors import KafkaError
from logging_utils import LoggerBase
import time
import json

class producerJsonKafka(LoggerBase):
    def __init__(self, bootstrap_server: str, topic: str, id: int,
                 name: str = "producerKafka", root: str = "./logs/"):
        """
        Initialize a Kafka producer for JSON messages.

        Args:
            bootstrap_server (str): The Kafka bootstrap server.
            topic (str): The name of the topic to produce messages to.
            id (int): A unique identifier for the producer.
            name (str, optional): The name of the producer. Defaults to "producerKafka".
            root (str, optional): The root directory for logging. Defaults to "./logs/".
        """
        # Call the constructor of the parent class
        super().__init__(name=name + str(id), root=root)

        # Store the Kafka bootstrap server and topic
        self._bootstrap_server = bootstrap_server
        self._topic = topic

        # Create a Kafka producer
        self._producer = KafkaProducer(
            bootstrap_servers=[self._bootstrap_server],
            retries=5,
            value_serializer=lambda m: json.dumps(m).encode('utf-8')  # Serialize JSON messages
        )
    
    def on_send_success(self, record_metadata: 'kafka.client_async.RecordMetadata') -> None:
        """
        Log the successful send details.

        Args:
            record_metadata (kafka.client_async.RecordMetadata): Metadata for the sent message.

        Returns:
            None
        """
        # Log the details of the successful send
        self.logger.info(
            'send success '  # Message format
            'topic: {}, '  # Topic name
            'partition: {}, '  # Partition number
            'offset: {}'  # Offset of the message
            .format(
                record_metadata.topic,  # Topic name
                record_metadata.partition,  # Partition number
                record_metadata.offset  # Offset of the message
            )
        )

    def on_send_error(self, excp):
        self.logger.error('I am an errback', exc_info=excp)
    # handle exception
    
    def push_data(self, data:dict):
        """
        Send a message to Kafka.

        Args:
            data (dict): The data to be sent.

        Returns:
            None
        """
        # Log the data being sent
        self.logger.info("push data {}".format(data))

        # Send the data to Kafka
        self.producer.send(self.topic, data) \
            .add_callback(self.on_send_success) \
            .add_errback(self.on_send_error)

        # Flush the producer to ensure all messages are sent
        self.producer.flush()

if __name__ == '__main__':
    
    producer_ = producerJsonKafka(
                                  bootstrap_server="0.0.0.0:9092",
                                  topic="cnx_test",
                                  id=1
                                )
    id = 0
    while True:
        time.sleep(2)
        producer_.push_data({"id":0,"content":"test nao {}".format(id)})
        id+=1

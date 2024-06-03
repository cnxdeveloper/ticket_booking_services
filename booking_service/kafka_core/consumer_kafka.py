from kafka import KafkaConsumer
from kafka.errors import KafkaError
from logging_utils import LoggerBase
import json

class consumerJsonKafka(LoggerBase):
    def __init__(self, bootstrap_server, grp_id, id, topic="", name="consumerKafka", auto_offset_reset='earliest', root='./logs/'):
        """
        Initialize a Kafka consumer for JSON data.

        Args:
            bootstrap_server (str): The Kafka bootstrap server.
            grp_id (str): The consumer group ID.
            id (int): The consumer ID.
            topic (str, optional): The Kafka topic to consume from. Defaults to "".
            name (str, optional): The log file name. Defaults to "consumerKafka".
            auto_offset_reset (str, optional): The auto offset reset policy. Defaults to 'earliest'.
            root (str, optional): The log file root directory. Defaults to './logs/'.
        """
        # Construct the log file name
        name += '{}_{}_{}'.format(grp_id, id, topic)
        super().__init__(name, root)

        # Initialize the Kafka consumer
        if topic != '':
            # If a topic is specified, consume from it
            self.consumer = KafkaConsumer( 
                                    topic,
                                    group_id=grp_id,
                                    bootstrap_servers=[bootstrap_server],
                                    value_deserializer=lambda m: json.loads(m.decode('utf8')),
                                    auto_offset_reset = auto_offset_reset
                                    )
        else:
            # If no topic is specified, consume from all topics
            self.consumer = KafkaConsumer( 
                                    group_id=grp_id,
                                    bootstrap_servers=[bootstrap_server],
                                    value_deserializer=lambda m: json.loads(m.decode('utf8')),
                                    auto_offset_reset = auto_offset_reset
                                    )


if __name__ == '__main__':
    consumer_process = consumerJsonKafka(
                                            bootstrap_server="0.0.0.0:9092",
                                            topic="cnx_test",
                                            grp_id="chungnx grp",
                                            id=0
                                        )
        
    for mess in consumer_process.consumer:
        print(mess)
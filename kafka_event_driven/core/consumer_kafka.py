from kafka import KafkaConsumer
from kafka.errors import KafkaError
from logging_utils import LoggerBase
import json

class consumerJsonKafka(LoggerBase):
    def __init__(self, bootstrap_server, topic, grp_id, id, name="consumerKafka", auto_offset_reset='earliest', root='./logs/'):
        name += '{}_{}_{}'.format(grp_id, id, topic)
        super().__init__(name, root)
        if topic != '':
            self.consumer = KafkaConsumer( 
                                        topic,
                                        group_id=grp_id,
                                        bootstrap_servers=[bootstrap_server],
                                        value_deserializer=lambda m: json.loads(m.decode('utf8')),
                                        auto_offset_reset = auto_offset_reset
                                        )
        else:
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
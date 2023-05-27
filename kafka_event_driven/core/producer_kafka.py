from kafka import KafkaProducer
from kafka.errors import KafkaError
from logging_utils import LoggerBase
import time
import json

class producerJsonKafka(LoggerBase):
    def __init__(self, bootstrap_server, topic, id, name="producerKafka", root='./logs/'):
        name += str(id)
        super().__init__(name, root)
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.producer = KafkaProducer(
                                      bootstrap_servers=[self.bootstrap_server],
                                      retries=5,
                                      value_serializer=lambda m: json.dumps(m).encode('utf8')
                                      )
    
    def on_send_success(self, record_metadata):
        self.logger.info('send success topic: {}, partition: {}, offset: {}'.format(
                                                                                    record_metadata.topic,
                                                                                    record_metadata.partition,
                                                                                    record_metadata.offset
                                                                                    ))

    def on_send_error(self, excp):
        self.logger.error('I am an errback', exc_info=excp)
    # handle exception
    
    def push_data(self, data:dict):
        self.logger.info("push data {}".format(data))
        self.producer.send(self.topic, data).add_callback(self.on_send_success).add_errback(self.on_send_error)
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

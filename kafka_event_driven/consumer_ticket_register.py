from core.consumer_kafka import consumerJsonKafka
from kafka import  TopicPartition
from mongo_client.mongodb_reservation import MongoDBReservation
from core.producer_kafka import producerJsonKafka
from kafka.admin import KafkaAdminClient, NewTopic
import uuid
import time


def init_ticket(numb_ticket=10):
    producer_kafka = producerJsonKafka(
                                  bootstrap_server="host.docker.internal:9092",
                                  topic="tickets_in_stock",
                                  id=1
                                )
    for i in range(numb_ticket):
        ticket_id = str(uuid.uuid4())
        producer_kafka.push_data(
                             {
                              "ticket_id": ticket_id,
                              "time_stamp": time.time()
                             }
                             )

def main():
    """
    This function is the main function that runs the consumer_ticket_register.py script.
    It initializes the MongoDB client, creates the necessary topics, and starts the consumers and producers.
    It also handles the different types of messages received from the consumer_process and performs the necessary operations.
    """
    # Initialize the MongoDB client
    mongo_db_client = MongoDBReservation(
                                     hostname="host.docker.internal",
                                     port=27017,
                                     user="root",
                                     pwd="admin@root",
                                     primary_key="reservation_id"
                                    )
    db_name = "booking_ticket_db"
    collection_name = "reservations_collect"
    # Initialize the consumer_process and consumer_ticket consumers
    consumer_process = consumerJsonKafka(
                                            bootstrap_server="host.docker.internal:9092",
                                            topic="register_ticket",
                                            grp_id="test_grp",
                                            id=1
                                        )
    
    consumer_ticket = consumerJsonKafka(
                                            bootstrap_server="host.docker.internal:9092",
                                            topic="",
                                            grp_id="grp_ticket",
                                            id=1
                                        )
    # Initialize the producer_ticket producer
    producer_ticket = producerJsonKafka(
                                  bootstrap_server="host.docker.internal:9092",
                                  topic="tickets_in_stock",
                                  id=1
                                )
    # Assign the consumer_ticket consumer to the tickets_in_stock topic
    partions_app = TopicPartition("tickets_in_stock", 0)
    consumer_ticket.consumer.assign([partions_app])
    
    # Initialize the tickets_in_stock topic with 2 tickets
    init_ticket(2)
    for i in range(2):
        ticket_info = consumer_ticket.consumer.poll(timeout_ms=1, max_records=1)
        consumer_ticket.consumer.commit()
    # Process the messages received from the consumer_process consumer
    for mess in consumer_process.consumer:
        if mess.value["request_type"] == "register":
            # If the request type is register, update the reservation in the MongoDB database
            consumer_process.logger.info("get message with content: {} ".format(mess))
            next_offsets = consumer_ticket.consumer.position(partions_app)
            end_offsets = consumer_ticket.consumer.end_offsets([partions_app]).get(partions_app)
            # Check if there are any tickets left
            uuid_ticket = None
            print(end_offsets - next_offsets - 1)
            if end_offsets - next_offsets - 1> 0:
                ticket_info = consumer_ticket.consumer.poll(timeout_ms=1, max_records=1)
                consumer_ticket.consumer.commit()
                if len(ticket_info) > 0:
                    data_ticket = ticket_info[partions_app][0].value
                    uuid_ticket = data_ticket["ticket_id"]
            if uuid_ticket is None:
                dict_data = {
                    "reservation_id": mess.value["reservation_id"],
                    "status": "out_of_stock",
                    "payment": None,
                    "uuid_ticket": None,
                }
            else:
                dict_data = {
                    "reservation_id": mess.value["reservation_id"],
                    "status": "waiting_payment",
                    "payment": "unknown",
                    "uuid_ticket": uuid_ticket,
                }
            mongo_db_client.update_data(db_name, collection_name, dict_data)
        elif mess.value["request_type"] == "payment":
            # If the request type is payment, update the reservation in the MongoDB database
            dict_data = {
                    "reservation_id": mess.value["reservation_id"],
                    "status": "success",
                    "payment": mess.value["payment_method"],
                    "uuid_ticket": uuid_ticket,
                }
            mongo_db_client.update_data(db_name, collection_name, dict_data)
        
        elif mess.value["request_type"] == "cancel":
            # If the request type is cancel, update the reservation in the MongoDB database
            reservation_data = mongo_db_client.get_data(db_name, collection_name, mess.value["reservation_id"])
            uuid_ticket = reservation_data["uuid_ticket"]
            print(reservation_data)
            if reservation_data["payment"] != "unknown":
                reservation_data["payment"] = "refund_payment"
            dict_data = {
                    "reservation_id": mess.value["reservation_id"],
                    "status": "canceled",
                    "payment": reservation_data["payment"]
            }
            mongo_db_client.update_data(db_name, collection_name, dict_data)
             ### reload ticket
            producer_ticket.push_data(
                                {
                                "ticket_id": uuid_ticket,
                                "time_stamp": time.time()
                                }
                             )
        


if __name__ == '__main__':
    main()
    
            
        
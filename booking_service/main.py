import requests
import uvicorn
from fastapi import FastAPI, File, UploadFile, BackgroundTasks, Body
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from typing import Optional

from fastapi.responses import JSONResponse
from kafka_core.producer_kafka import producerJsonKafka
from kafka_core.consumer_kafka import consumerJsonKafka
from mongo_client.mongodb_reservation import MongoDBReservation
from kafka import  TopicPartition
import time
import uuid


class RegisterTicket(BaseModel):
    customer: str
    ticketId: str
    requestId: Optional[str] = ""


class CancelTicket(BaseModel):
    reservation_id: str
    requestId: Optional[str] = ""

class Payment(BaseModel):
    reservation_id: str
    payment_method: str                                                                   

app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
mongo_db_client = MongoDBReservation(
                                     hostname="host.docker.internal",
                                     port=27017,
                                     user="root",
                                     pwd="admin@root",
                                     primary_key="reservation_id"
                                    )
db_name = "booking_ticket_db"
collection_name = "reservations_collect"

############################### REST FULL API ##########################################

def numb_ticket_in_stock():
    """
    Function to get the number of tickets in stock.

    This function uses a Kafka consumer to get the current position and end offsets for the tickets_in_stock topic.
    It calculates the number of tickets in stock by subtracting the current position from the end offset.

    Returns:
        int: The number of tickets in stock.
    """
    # Create a Kafka consumer
    consumer_ticket = consumerJsonKafka(
        bootstrap_server="host.docker.internal:9092",  # Kafka bootstrap server
        grp_id="grp_ticket",  # Kafka consumer group ID
        id=1  # Kafka consumer ID
    )

    # Assign the tickets_in_stock topic to the consumer
    partions_app = TopicPartition("tickets_in_stock", 0)
    consumer_ticket.consumer.assign([partions_app])

    # Get the current position and end offsets for the tickets_in_stock topic
    next_offsets = consumer_ticket.consumer.position(partions_app)  # Current position
    end_offsets = consumer_ticket.consumer.end_offsets([partions_app]).get(partions_app)  # End offset

    # Calculate the number of tickets in stock
    numb_ticket = end_offsets - next_offsets - 1

    # Print the current position and end offsets
    print(next_offsets, end_offsets)

    # Return the number of tickets in stock
    return numb_ticket

def add_ticket(numb_ticket=10):
    """
    Function to add tickets to the tickets_in_stock topic.

    This function creates a Kafka producer and sends a dictionary containing a unique ticket ID and the current timestamp
    to the tickets_in_stock topic.

    Args:
        numb_ticket (int): The number of tickets to add. Default is 10.

    Returns:
        None
    """
    # Create a Kafka producer
    producer_kafka = producerJsonKafka(
        bootstrap_server="host.docker.internal:9092",  # Kafka bootstrap server
        topic="tickets_in_stock",  # Kafka topic to send the tickets to
        id=1  # Kafka producer ID
    )

    # Loop through the number of tickets to add
    for i in range(numb_ticket):
        # Create a unique ticket ID
        ticket_id = str(uuid.uuid4())

        # Create a dictionary with the ticket ID and current timestamp
        data = {
            "ticket_id": ticket_id,
            "time_stamp": time.time()
        }

        # Send the dictionary to the Kafka topic
        producer_kafka.push_data(data)


@app.post("/register")
async def register_ticket(background_tasks: BackgroundTasks, request: RegisterTicket):
    """
    Register a ticket for a customer.

    This function creates a unique reservation ID, registers the customer's details in the MongoDB database, and sends a
    JSON message to the "register_ticket" Kafka topic.

    Args:
        background_tasks (BackgroundTasks): A collection of tasks to be executed asynchronously.
        request (RegisterTicket): The request object containing the customer's details.

    Returns:
        JSONResponse: A JSON response containing the status and reservation ID.
    """
    # Create a unique reservation ID
    reservation_id = str(uuid.uuid4())

    # Create a unique ID for the Kafka message
    id_message = "mess" + str(uuid.uuid4())

    # Check if there are any tickets in stock
    if numb_ticket_in_stock() <= 0:
        return JSONResponse({
            "status": 301,  # Ticket out of stock
            "msg": "ticket out of stock",
            "reservation_id": None
        })

    # Create a dictionary with the customer's details and reservation ID
    dict_data = {
        "reservation_id": reservation_id,
        "status": "waiting",
        "customer": request.customer,
        "payment": None,
        "uuid_ticket": None,
        "created_date": time.time()
    }

    # Update the MongoDB database with the customer's details
    mongo_db_client.update_data(db_name, collection_name, dict_data)

    # Create a Kafka producer and send a JSON message to the "register_ticket" topic
    producer_kafka = producerJsonKafka(
        bootstrap_server="host.docker.internal:9092",
        topic="register_ticket",
        id=1
    )
    producer_kafka.push_data(
        {
            "id_message": id_message,
            "request_type": "register",
            "ticket_id": request.ticketId,
            "customer": request.customer,
            "reservation_id": reservation_id
        }
    )

    # Return a JSON response with the status and reservation ID
    return JSONResponse({
        "status": 200,  # Success
        "msg": "successfully register ticket",
        "reservation_id": reservation_id
    })


@app.post("/payment")
async def payment_ticket(background_tasks: BackgroundTasks, request: Payment):
    """
    Handle the payment request for a reservation.

    Args:
        background_tasks (BackgroundTasks): Background tasks collection.
        request (Payment): Payment request object.

    Returns:
        JSONResponse: JSON response indicating the status and result of the payment.
    """
    # List of supported payment methods
    list_bank_method = ["bank_transfer", "visa"]

    # Retrieve reservation data from the database
    reservation_data = mongo_db_client.get_data(db_name, collection_name, request.reservation_id)
    print(reservation_data)

    # Check if reservation ID is valid
    if reservation_data is None:
        result = {
            "status": 302,
            "msg": "invalid reservation_id"
        }
    else:
        # Check if reservation is in waiting_payment status
        if reservation_data['status'] != "waiting_payment":
            result = {
                "status": 303,
                "msg": "you cannot payment"
            }
        else:
            # Check if payment method is supported
            if request.payment_method in list_bank_method:
                id_message = "mess" + str(uuid.uuid4())
                dict_data = {
                    "reservation_id": request.reservation_id,
                    "status": "payment_processing"
                }
                # Update reservation status in the database
                mongo_db_client.update_data(db_name, collection_name, dict_data)
                # Create a Kafka producer and send a JSON message to the "register_ticket" topic
                producer_kafka = producerJsonKafka(
                                    bootstrap_server="host.docker.internal:9092",
                                    topic="register_ticket",
                                    id=1
                                    )
                producer_kafka.push_data(
                                {
                                "id_message":  id_message,
                                "request_type": "payment",
                                "payment_method": request.payment_method,
                                "reservation_id": request.reservation_id
                                }
                             )
                result = {
                    "status": 200,
                    "msg": "success payment"
                }
            else:
                result = {
                    "status": 304,
                    "msg": "payment method invalid"
                }
    return JSONResponse(result)
    

@app.post("/cancel")
async def cancel_ticket(background_tasks: BackgroundTasks, request: CancelTicket):
    """
    Cancel a ticket reservation.

    This function retrieves the reservation data from the MongoDB database, updates the reservation status to
    "cancel_processing", creates a Kafka message with the reservation ID and request type, and sends the message to the
    "register_ticket" Kafka topic.

    Args:
        background_tasks (BackgroundTasks): A collection of tasks to be executed asynchronously.
        request (CancelTicket): The request object containing the reservation ID.

    Returns:
        JSONResponse: A JSON response indicating the status and result of the cancellation.
    """
    # Retrieve reservation data from the database
    reservation_data = mongo_db_client.get_data(db_name, collection_name, request.reservation_id)
    
    # Check if reservation ID is valid
    if reservation_data is None:
        result = {
            "status": 302,
            "msg": "invalid reservation_id"
        }
    else:
        # Update reservation status in the database
        dict_data = {
            "reservation_id": request.reservation_id,
            "status": "cancel_processing"
        }
        mongo_db_client.update_data(db_name, collection_name, dict_data)
        
        # Create a Kafka message with the reservation ID and request type
        id_message = "mess" + str(uuid.uuid4())
        producer_kafka = producerJsonKafka(
            bootstrap_server="host.docker.internal:9092",
            topic="register_ticket",
            id=1
        )
        producer_kafka.push_data(
            {
                "id_message":  id_message,
                "request_type": "cancel",
                "reservation_id": request.reservation_id
            }
        )
        
        # Return a JSON response indicating the status and result of the cancellation
        result = {
            "status": 200,
            "msg": "Cancel order"
        }
    
    return JSONResponse(result)


@app.get('/status')
async def status(reservation_id: str):
    """
    Get the status of a reservation.

    This function retrieves the reservation data from the MongoDB database based on the reservation ID.
    If the reservation ID is valid, it returns the status and reservation information.
    If the reservation ID is invalid, it returns an error message.

    Args:
        reservation_id (str): The ID of the reservation.

    Returns:
        JSONResponse: A JSON response containing the status and reservation information.
    """
    # Retrieve reservation data from the database
    reservation_data = mongo_db_client.get_data(db_name, collection_name, reservation_id)
    
    # Check if reservation ID is valid
    if reservation_data is None:
        result = {
            "status": 302,
            "msg": "invalid reservation_id"
        }
    else:
        # Remove the '_id' field from the reservation data
        reservation_data.pop('_id')
        
        # Return a JSON response containing the status and reservation information
        result = {
            "status": 200,
            "reservation_info": reservation_data
        }
    
    return JSONResponse(result)

@app.get('/add_tickets')
async def add_tickets(number_ticket: int):
    """
    Add tickets to the tickets_in_stock topic.

    This function creates a Kafka producer and sends a dictionary containing a unique ticket ID and the current timestamp
    to the tickets_in_stock topic.

    Args:
        number_ticket (int): The number of tickets to add.

    Returns:
        JSONResponse: A JSON response indicating the status and message of the ticket addition.
    """
    # Add tickets to the tickets_in_stock topic
    add_ticket(number_ticket)

    # Create a JSON response indicating the status and message of the ticket addition
    result = {
            "status": 200,
            "msg": "add ticket success"
        }

    # Return the JSON response
    return JSONResponse(result)



@app.get('/tickets_remain')
async def status():
    """
    Get the number of tickets remaining in the stock.

    This function calls the `numb_ticket_in_stock` function to get the number of tickets remaining in the stock. It
    creates a JSON response containing the status and the number of tickets in stock.

    Returns:
        JSONResponse: A JSON response containing the status and the number of tickets in stock.
    """
    # Get the number of tickets remaining in the stock
    numb_ticket = numb_ticket_in_stock()

    # Create a JSON response containing the status and the number of tickets in stock
    result = {
            "status": 200,
            "ticket_in_stock": numb_ticket
        }

    # Return the JSON response
    return JSONResponse(result)


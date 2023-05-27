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
    print("is_ticket_out_stock")
    consumer_ticket = consumerJsonKafka(
                                            bootstrap_server="host.docker.internal:9092",
                                            grp_id="grp_ticket",
                                            id=1
                                        )
    partions_app = TopicPartition("tickets_in_stock", 0)
    consumer_ticket.consumer.assign([partions_app])
    next_offsets = consumer_ticket.consumer.position(partions_app)
    end_offsets = consumer_ticket.consumer.end_offsets([partions_app]).get(partions_app)
    print(next_offsets, end_offsets)
    return end_offsets - next_offsets - 1

def add_ticket(numb_ticket=10):
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


@app.post("/register")
async def register_ticket(background_tasks: BackgroundTasks, request: RegisterTicket):
    print("register_ticket")
    reservation_id = str(uuid.uuid4())
    id_message = "mess" + str(uuid.uuid4())
    if numb_ticket_in_stock() <= 0:
        return JSONResponse({
            "status": 301,
            "msg": "ticket out of stock",
            "reservation_id": None
        })
    dict_data = {
        "reservation_id":reservation_id,
        "status": "waiting",
        "customer": request.customer,
        "payment": None,
        "uuid_ticket": None,
        "created_date": time.time()
    }
    print("chungnx", dict_data)
    mongo_db_client.update_data(db_name, collection_name, dict_data)
    producer_kafka = producerJsonKafka(
                                  bootstrap_server="host.docker.internal:9092",
                                  topic="register_ticket",
                                  id=1
                                )
    producer_kafka.push_data(
                             {
                              "id_message":  id_message,
                              "request_type": "register",
                              "ticket_id": request.ticketId,
                              "customer": request.customer,
                              "reservation_id": reservation_id
                              }
                             )
    
    return JSONResponse({
        "status": 200,
        "msg": "successfully register ticket",
        "reservation_id": reservation_id
    })


@app.post("/payment")
async def payment_ticket(background_tasks: BackgroundTasks, request: Payment):
    list_bank_method = ["bank_transfer", "visa"]
    reservation_data = mongo_db_client.get_data(db_name, collection_name, request.reservation_id)
    print(reservation_data)
    if reservation_data is None:
        result = {
            "status": 302,
            "msg": "invalid reservation_id"
        }
    else:
        if reservation_data['status'] != "waiting_payment":
            result = {
                "status": 303,
                "msg": "you cannot payment"
            }
        else:
            if request.payment_method in list_bank_method:
                id_message = "mess" + str(uuid.uuid4())
                dict_data = {
                    "reservation_id": request.reservation_id,
                    "status": "payment_processing"
                }
                mongo_db_client.update_data(db_name, collection_name, dict_data)
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
    #BOOKING ORDER
    reservation_data = mongo_db_client.get_data(db_name, collection_name, request.reservation_id)
    if reservation_data is None:
        result = {
            "status": 302,
            "msg": "invalid reservation_id"
        }
    else:
        dict_data = {
                    "reservation_id": request.reservation_id,
                    "status": "cancel_processing"
                    }
        mongo_db_client.update_data(db_name, collection_name, dict_data)
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
        result = {
            "status": 200,
            "msg": "Cancel order"
        }
    return JSONResponse(result)


@app.get('/status')
async def status(reservation_id: str):
    reservation_data = mongo_db_client.get_data(db_name, collection_name, reservation_id)
    print(reservation_data)
    if reservation_data is None:
        result = {
            "status": 302,
            "msg": "invalid reservation_id"
        }
    else:
        reservation_data.pop('_id')
        print("-----------------------------------")
        print(reservation_data)
        result = {
            "status": 200,
            "reservation_info": reservation_data
        }
    return JSONResponse(result)

@app.get('/add_tickets')
async def status(number_ticket: int):
    add_ticket(number_ticket)
    result = {
            "status": 200,
            "msg": "add ticket success"
        }
    return JSONResponse(result)

@app.get('/tickets_remain')
async def status():
    numb_ticket = numb_ticket_in_stock()
    result = {
            "status": 200,
            "ticket_in_stock": numb_ticket
        }
    return JSONResponse(result)


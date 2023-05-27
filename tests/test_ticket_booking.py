import requests
import time
## run local enable this
# host_name = "http://0.0.0.0:8003"
## run with docker enable this
host_name = "http://host.docker.internal:8003"

def add_tikets_get(number):
    resp = requests.get(f'{host_name}/add_tickets?number_ticket={number}')
    return resp

def get_tickets_remain():
    resp = requests.get(f'{host_name}/tickets_remain')
    return resp

def register_ticket(user, ticket_id):
    data = {'customer': user, 'ticketId': ticket_id}
    resp = requests.post(f'{host_name}/register', json=data)
    return resp

def payment_reservation(reservation_id, payment_method):
    data ={"payment_method": payment_method, "reservation_id": reservation_id}
    resp = requests.post(f'{host_name}/payment', json=data)
    return resp

def get_status_reservation(reservation_id):
    resp = requests.get(f'{host_name}/status?reservation_id={reservation_id}')
    return resp

def cancel_order(reservation_id):
    data ={"reservation_id": reservation_id}
    resp = requests.post(f'{host_name}/cancel', json=data)
    return resp
    

def test_booking_system():
    #init with 3 tickets
    #test api add tickets
    number_ticket = 3
    resp = add_tikets_get(number_ticket)
    assert resp.status_code == 200
    assert resp.json()["msg"] == "add ticket success"
    
    #test api get ticket remain
    resp = get_tickets_remain()
    assert resp.status_code == 200
    assert resp.json()["ticket_in_stock"] == number_ticket
    
    #test api register 
    user_name = ["test_user1", "test_user2", "test_user3"]
    ticket_id = "music-ticket"
    reservation_ids = []
    resp = register_ticket(user_name[0], ticket_id)
    assert resp.status_code == 200
    assert resp.json()["status"] == 200
    reservation_ids.append(resp.json()["reservation_id"])
    
    resp = register_ticket(user_name[1], ticket_id)
    assert resp.status_code == 200
    assert resp.json()["status"] == 200
    reservation_ids.append(resp.json()["reservation_id"])
    
    resp = register_ticket(user_name[2], ticket_id)
    assert resp.status_code == 200
    assert resp.json()["status"] == 200
    reservation_ids.append(resp.json()["reservation_id"])
    
    #test ticket remain
    resp = get_tickets_remain()
    print(reservation_ids)
    assert resp.status_code == 200
    assert resp.json()["ticket_in_stock"] == 0
    
    #test cannot register when ticket out of stock rais status 301
    resp = register_ticket("test4", ticket_id)
    assert resp.status_code == 200
    assert resp.json()["status"] == 301
    assert resp.json()["msg"] == "ticket out of stock"
    
    
    #test status register ticket
    resp = get_status_reservation(reservation_ids[0])
    assert resp.status_code == 200
    assert resp.json()["reservation_info"]["customer"] == user_name[0]
    assert resp.json()["reservation_info"]["status"] == "waiting_payment"
    
    resp = get_status_reservation(reservation_ids[1])
    assert resp.status_code == 200
    assert resp.json()["reservation_info"]["customer"] == user_name[1]
    assert resp.json()["reservation_info"]["status"] == "waiting_payment"
    
    resp = get_status_reservation(reservation_ids[2])
    assert resp.status_code == 200
    assert resp.json()["reservation_info"]["customer"] == user_name[2]
    assert resp.json()["reservation_info"]["status"] == "waiting_payment"
    
    #test payment just support 2 method visa and bank_transfer
    resp = payment_reservation(reservation_ids[0], "visa")
    assert resp.status_code == 200
    assert resp.json()["status"] == 200
    
    resp = get_status_reservation(reservation_ids[0])
    assert resp.status_code == 200
    assert resp.json()["reservation_info"]["customer"] == user_name[0]
    assert resp.json()["reservation_info"]["status"] == "success"
    assert resp.json()["reservation_info"]["payment"] == "visa"
    
    resp = payment_reservation(reservation_ids[1], "bank_transfer")
    assert resp.status_code == 200
    assert resp.json()["status"] == 200
    
    resp = get_status_reservation(reservation_ids[1])
    assert resp.status_code == 200
    assert resp.json()["reservation_info"]["customer"] == user_name[1]
    assert resp.json()["reservation_info"]["status"] == "success"
    assert resp.json()["reservation_info"]["payment"] == "bank_transfer"
    
    #test invalid payment raise code 304
    resp = payment_reservation(reservation_ids[2], "cash")
    assert resp.status_code == 200
    assert resp.json()["status"] == 304
    
    #test cannot payment for order already paid raise code 303
    resp = payment_reservation(reservation_ids[1], "visa")
    assert resp.status_code == 200
    assert resp.json()["status"] == 303
    
    #test cancel with order already paid
    resp = cancel_order(reservation_ids[1])
    assert resp.status_code == 200
    assert resp.json()["status"] == 200
    assert resp.json()["msg"] == "Cancel order"
    #check status has payment  been refunded yet
    time.sleep(1)
    resp = get_status_reservation(reservation_ids[1])
    assert resp.status_code == 200
    assert resp.json()["reservation_info"]["customer"] == user_name[1]
    assert resp.json()["reservation_info"]["status"] == "canceled"
    assert resp.json()["reservation_info"]["payment"] == "refund_payment"
    
    #test cancel with order hasn't payment
    resp = cancel_order(reservation_ids[2])
    assert resp.status_code == 200
    assert resp.json()["status"] == 200
    assert resp.json()["msg"] == "Cancel order"
    #check status has payment  been refunded yet
    time.sleep(1)
    resp = get_status_reservation(reservation_ids[2])
    assert resp.status_code == 200
    assert resp.json()["reservation_info"]["customer"] == user_name[2]
    assert resp.json()["reservation_info"]["status"] == "canceled"
    assert resp.json()["reservation_info"]["payment"] == "unknown"
    
    #check system auto reload 2 ticket canceled
    resp = get_tickets_remain()
    assert resp.status_code == 200
    assert resp.json()["ticket_in_stock"] == 2
    
    
    
    
    
    
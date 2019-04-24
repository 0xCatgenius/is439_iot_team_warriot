from twilio.rest import Client
from store_Sensor_Data_to_DB import init_db

init_db()

def sendMessage(number, pi):
# Your Account Sid and Auth Token from twilio.com/console
    account_sid = 'AC02f01a5914a7056a8648714ee5cbdbb6'
    auth_token = 'f03e5d21b8f8e8b965d349c8a2960dca'
    client = Client(account_sid, auth_token)
    message1 = 'Welcome to Fabriqade! Enjoy 10% today @ *Scape by flashing your e-membership card!'
    message2 = 'Welcome to ducks&crafts! Bring a friend with you and enjoy 10% for both of our purchase!'
    sms = 'This is a default message.'
    if pi == 'fabriqade':
        sms = message1
    else:
        sms = message2

    #print(number)
    #print(sms)
    message = client.messages \
        .create(
            body=sms,
            from_='+17243132892',
            to= number
        )

    #print(message.sid)


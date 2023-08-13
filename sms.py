from twilio.rest import Client
from configparser import ConfigParser

CONFIG = ConfigParser()
CONFIG.read('config.ini')

# Your Twilio Account SID and Auth Token
SID = CONFIG.get('twilio', 'sid')
TOKEN = CONFIG.get('twilio', 'token')
FROM_NUMBER = CONFIG.get('twilio', 'from_number')
TO_NUMBER = CONFIG.get('twilio', 'to_number')

# Create a Twilio client
CLIENT = Client(SID, TOKEN)




def send_sms(message_body):
    message = CLIENT.messages.create(
                                    body=message_body,
                                    from_=FROM_NUMBER,
                                    to=TO_NUMBER
                                    )
    return f"SMS sent with SID: {message.sid}"
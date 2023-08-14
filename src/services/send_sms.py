from twilio.rest import Client
import os
from services.utils import load_json

CONFIG_PATH = os.path.join("configuration", "config.json")
CONFIG = load_json(CONFIG_PATH)

# Your Twilio Account SID and Auth Token
SID = CONFIG.get('twilio').get('sid')
TOKEN = CONFIG.get('twilio').get('token')
FROM_NUMBER = CONFIG.get('twilio').get('from_number')
TO_NUMBER = CONFIG.get('twilio').get('to_number')

# Create a Twilio client
CLIENT = Client(SID, TOKEN)

def send_sms(message_body):
    print(f"message body: {message_body}")
    message = CLIENT.messages.create(
                                    body=message_body,
                                    from_=FROM_NUMBER,
                                    to=TO_NUMBER
                                    )
    return f"SMS sent with SID: {message.sid}"

import base64
import os
from mailjet_rest import Client
from services.utils import load_json

CONFIG_PATH = os.path.join("configuration", "config.json")
CONFIG = load_json(CONFIG_PATH)


# Get your environment Mailjet keys
API_KEY = CONFIG.get('mailjet').get('api_key')
API_SECRET = CONFIG.get('mailjet').get('secret_key')

SENDER_EMAIL = CONFIG.get('mailjet').get('sender_email')
RECIPIENT_EMAIL = 'michaelthorsparre@gmail.com'
SUBJECT = 'FPL Draft Daily Report'

MAILJET = Client(auth=(API_KEY, API_SECRET), version="v3.1")


def send_email(attachment, message_body):
    with open(attachment, 'rb') as attachment_file:
        attachment_data = base64.b64encode(attachment_file.read()).decode('utf-8')

    data = {
        'Messages': [
            {
            "From": {
                "Email": SENDER_EMAIL,
                "Name": "Michael Sparre"
            },
            "To": [
                {
                "Email": RECIPIENT_EMAIL,
                "Name": "Michael Sparre"
                }
            ],
            "Subject": SUBJECT,
            "TextPart": message_body,
            "Attachments": [
                {
                    "ContentType" : "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # Content type for Excel files
                    "Filename": attachment,
                    "Base64Content": attachment_data
                }
            ]
            }
        ]
        }
    result = MAILJET.send.create(data=data)
    print(result.status_code)
    print(result.json())
    return


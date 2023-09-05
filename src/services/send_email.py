
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

# sends email to newsletter recipients
def send_email(attachment_files, message_body):
    if len(attachment_files) == 0: return "no updates to send"
    attachments = []
    for a in attachment_files:
        with open(a, 'rb') as attachment_file:
            attachments.append(base64.b64encode(attachment_file.read()).decode('utf-8'))

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
            "TextPart": "FPL Newsletter\n" + message_body,
            "Attachments": [
                {
                    "ContentType" : "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # Content type for Excel files
                    "Filename": os.path.basename(filename),
                    "Base64Content": a
                } for filename, a in zip(attachment_files, attachments)
            ]
            }
        ]
        }
    result = MAILJET.send.create(data=data)
    return result.json()


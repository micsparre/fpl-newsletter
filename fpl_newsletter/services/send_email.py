
import base64
import os
from mailjet_rest import Client
from services.utils import load_json

CONFIG_PATH = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), "..", "configuration", "config.json")
CONFIG = load_json(CONFIG_PATH)


# Get your environment Mailjet keys
API_KEY = CONFIG.get('mailjet').get('api_key')
API_SECRET = CONFIG.get('mailjet').get('secret_key')
SENDER_EMAIL = CONFIG.get('mailjet').get('sender_email')

SUBJECT = 'FPL Newsletter'

MAILJET = Client(auth=(API_KEY, API_SECRET), version="v3.1")


def send_email(attachment_files, message_body, recipient_email, recipient_name):
    """
    Sends an email with the given attachments
    """
    if len(attachment_files) == 0:
        return "no updates to send"
    attachments = []
    for a in attachment_files:
        with open(a, 'rb') as attachment_file:
            attachments.append(base64.b64encode(
                attachment_file.read()).decode('utf-8'))

    data = {
        'Messages': [
            {
                "From": {
                    "Email": SENDER_EMAIL,
                    "Name": "FPL Newsletter"
                },
                "To": [
                    {
                        "Email": recipient_email,
                        "Name": recipient_name
                    }
                ],
                "Bcc": [
                    {
                        "Email": "fantasyplnewsletter@gmail.com",
                        "Name": "Michael Sparre"
                    }
                ],
                "Subject": SUBJECT,
                "TextPart": message_body,
                "Attachments": [
                    {
                        # Content type for Excel files
                        "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if filename.endswith(".xlsx") else "image/png",
                        "Filename": os.path.basename(filename),
                        "Base64Content": a
                    } for filename, a in zip(attachment_files, attachments)
                ]
            }
        ]
    }
    result = MAILJET.send.create(data=data)
    return result.json()

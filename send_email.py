
import base64
from mailjet_rest import Client
from configparser import ConfigParser

CONFIG = ConfigParser()
CONFIG.read('config.ini')

# Get your environment Mailjet keys
API_KEY = CONFIG.get('mailjet', 'api_key')
API_SECRET = CONFIG.get('mailjet', 'secret_key')

smtp_username= CONFIG.get('mailjet', 'username')
smtp_password = CONFIG.get('mailjet', 'password')

SENDER_EMAIL = CONFIG.get('mailjet', 'sender_email')
RECIPIENT_EMAIL = 'michaelthorsparre@gmail.com'
SUBJECT = 'Test Email'
message = 'This is a test email sent using Mailjet SMTP.'

mailjet = Client(auth=(API_KEY, API_SECRET), version="v3.1")


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
    result = mailjet.send.create(data=data)
    print(result.status_code)
    print(result.json())
    return


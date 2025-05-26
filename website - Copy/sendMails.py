import requests
import msal
import json
import base64
# Detalii aplicație
tenant_id = '52089378-759b-4f24-a522-881ef92534ec'  # Înlocuiește cu Tenant ID-ul tău
client_id = '091ff128-d742-4f8d-96dc-2649866c782c'  # Înlocuiește cu Client ID-ul tău
client_secret = 'NGp8Q~mnVQk4UWfivh45XZM80~~IaXiWFJd7_ccv'  # Înlocuiește cu Client Secret-ul tău
SCOPE = ["https://graph.microsoft.com/.default"]
# URL de token
token_url = f'https://login.microsoftonline.com/{tenant_id}'

# Datele pentru autentificare
def get_access_token():
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret,
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" in result:
        return result['access_token']
    else:
        raise Exception("Failed to acquire token", result.get("error"), result.get("error_description"))

def send_email_via_graph_api(subject, recipient, body,attachment_path=None, cc_recipients=None):
    access_token = get_access_token()
    if attachment_path:
        with open(attachment_path, "rb") as attachment_file:
            attachment_content = base64.b64encode(attachment_file.read()).decode('utf-8')
        
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": attachment_path.split("/")[-1],  # Numele fișierului
            "contentBytes": attachment_content
        }
    else:
        attachment = None
    if cc_recipients:
        cc_list = [{"emailAddress": {"address": email}} for email in cc_recipients] 
    else :
        cc_list=None
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    email_data = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "from": {
                "emailAddress": {
                    "address": "gtrdigital@ro.gt.com"
                }
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": recipient
                    }
                }
            ]
        }
    }
    if cc_list:
            email_data["message"]["ccRecipients"] = cc_list

    # Adăugăm atașamentul, dacă există
    if attachment:
        email_data["message"]["attachments"] = [attachment]
    user_endpoint = f'https://graph.microsoft.com/v1.0/users/gtrdigital@ro.gt.com/sendMail'
    response = requests.post(
        user_endpoint,
        headers=headers,
        data=json.dumps(email_data)
    )
    if response.status_code != 202:
        raise Exception(f"Error sending email: {response.status_code} - {response.text}")



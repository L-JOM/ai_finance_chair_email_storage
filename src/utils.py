import requests
from datetime import datetime
from collections import defaultdict
from src import START_DATE
import pandas as pd
from bs4 import BeautifulSoup
from email_reply_parser import EmailReplyParser
import webbrowser
from urllib.parse import urlparse, parse_qs
from msal import ConfidentialClientApplication
from azure.identity import DefaultAzureCredential

GRAPH_SCOPE = "https://graph.microsoft.com/.default"

def get_token():
    credential = DefaultAzureCredential()
    token = credential.get_token(GRAPH_SCOPE)
    return token.token


TENANT_ID = "cbda322f-63a9-4608-982d-be7665a1e883"
CLIENT_ID = "46b84c6c-cc21-473f-9997-50de1b1167fe"
CLIENT_SECRET = "YXy8Q~OsI1R~fZokw69Cgm54etoPRHqv6OiYLdol"
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
REDIRECT_URI = "http://localhost:8000/callback"  # must match Azure app registration
SCOPES = ["User.Read", "Mail.ReadWrite"]
GRAPH_API = "https://graph.microsoft.com/v1.0/me"


def get_token_via_browser():
    # Step 1: Generate the auth URL and open it
    app = ConfidentialClientApplication(client_id=CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET)
    auth_url = app.get_authorization_request_url(SCOPES, redirect_uri=REDIRECT_URI)
    print("Sign in at this URL and copy the full redirect URL:")
    print(auth_url)
    webbrowser.open(auth_url)

    # Step 2: Paste back the redirect URL after login
    redirect_response = input("Paste the full redirect URL here: ")

    # Step 3: Parse the authorization code
    query = urlparse(redirect_response).query
    code = parse_qs(query).get("code")[0]    
    # Step 4: Exchange code for token
    access_token = app.acquire_token_by_authorization_code(code, scopes=SCOPES, redirect_uri=REDIRECT_URI)
    access_id = access_token['access_token']
    return access_id

def graph_get(url: str, token: str, params=None): 
    headers = {"Authorization": f"Bearer {token}"} 
    response = requests.get(url, headers=headers, params=params) 
    response.raise_for_status()
    return response.json()

def get_inbox_emails(start_date: datetime, token: str):
    inbox_msgs = []
    url = f"{GRAPH_API}/mailFolders/inbox/messages"
    params = {
        "$top": 50,
        "$orderby": "receivedDateTime asc",
        "$filter": (
            f"receivedDateTime ge {start_date.isoformat()}Z "
            f"and not(contains(from/emailAddress/address,'noreply')) "
            f"and not(contains(from/emailAddress/address,'no-reply')) "
            f"and not(contains(from/emailAddress/name,'Mail Delivery Subsystem')) "
            f"and not(contains(from/emailAddress/address,'@nsbe.org'))"
        )
    }

    while url:
        data = graph_get(url, token, params)
        for msg in data.get("value", []):
            sender_info = msg.get("from", {}).get("emailAddress", {})
            to_recipients = msg.get("toRecipients", [])
            if not sender_info or not to_recipients:
                continue
            inbox_msgs.append({
                "id": msg["id"],
                "conversationId": msg["conversationId"],
                "direction": "in",
                "datetime": msg["receivedDateTime"],
                "sender": sender_info.get("address", ""),
                "subject": msg.get("subject", ""),
                "body": msg.get("body", {}).get("content", ""),
                "to": [r["emailAddress"]["address"] for r in to_recipients]
            })
        url = data.get("@odata.nextLink")
        params = None
    return inbox_msgs

def get_sent_emails(start_date: datetime, token: str, my_email: str):
    sent_msgs = []
    url = f"{GRAPH_API}/mailFolders/sentitems/messages"
    params = {
        "$top": 50,
        "$orderby": "sentDateTime asc",
        "$filter": f"sentDateTime ge {start_date.isoformat()}Z"
    }

    while url:
        data = graph_get(url, token, params)
        for msg in data.get("value", []):
            to_recipients = msg.get("toRecipients", [])
            sent_msgs.append({
                "id": msg["id"],
                "conversationId": msg["conversationId"],
                "direction": "out",
                "datetime": msg["sentDateTime"],
                "sender": my_email,
                "subject": msg.get("subject", ""),
                "body": msg.get("body", {}).get("content", ""),
                "to": [r["emailAddress"]["address"] for r in to_recipients]
            })
        url = data.get("@odata.nextLink")
        params = None
    return sent_msgs

def sequential_pairing(start_date: datetime, my_email: str):
    token = get_token()  # your auth function
    inbox_msgs = get_inbox_emails(start_date, token)
    sent_msgs = get_sent_emails(start_date, token, my_email)
    # --- Group by conversationId ---
    conversations = defaultdict(list)
    for msg in inbox_msgs:
        conversations[msg["sender"]].append(msg)
    for msg in sent_msgs:
        for reciepient in msg["to"]:
            conversations[reciepient].append(msg)
    
    results = []
    for email, msgs in conversations.items():
        # sort by datetime
        msgs.sort(key=lambda x: x["datetime"])

        # look for pairs: incoming → next outgoing
        for i, msg in enumerate(msgs):
            if msg["direction"] == "in":
                in_time = datetime.strptime(msg["datetime"], "%Y-%m-%dT%H:%M:%SZ")
                for j in range(i + 1, len(msgs)):
                    reply_time = datetime.strptime(msgs[j]["datetime"], "%Y-%m-%dT%H:%M:%SZ")
                    if msgs[j]["direction"] == "out" and ((reply_time - in_time).days < 14):
                        incoming_body = BeautifulSoup(msg["body"], "html.parser").text
                        my_reply =  BeautifulSoup(msgs[j]["body"], "html.parser").text
                        results.append({
                            "sender": msg["sender"],
                            "subject": msg["subject"],
                            "incoming_body": incoming_body,
                            "your_reply": my_reply,
                            "reply_time" : msgs[j]["datetime"]
                        })
                        break  # only first reply per incoming
    return results

def create_email_csv(start_date, email):
    email_json = sequential_pairing(START_DATE, email)
    df = pd.DataFrame(email_json)
    return df

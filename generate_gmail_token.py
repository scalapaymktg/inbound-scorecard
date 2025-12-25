"""
One-time script to generate Gmail OAuth token.
Run locally: python generate_gmail_token.py
"""

import os
import json
import requests
import urllib.parse

CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
REDIRECT_URI = "https://grouper-fast-moderately.ngrok-free.app/rest/oauth2-credential/callback"

SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.modify',
    'https://www.googleapis.com/auth/spreadsheets'
]

def main():
    scope_str = ' '.join(SCOPES)
    auth_url = (
        f"https://accounts.google.com/o/oauth2/auth?"
        f"client_id={CLIENT_ID}&"
        f"redirect_uri={urllib.parse.quote(REDIRECT_URI)}&"
        f"response_type=code&"
        f"scope={urllib.parse.quote(scope_str)}&"
        f"access_type=offline&"
        f"prompt=consent"
    )

    print("1. Apri questo URL nel browser:\n")
    print(auth_url)
    print("\n2. Dopo il login, verrai rediretto all'URL ngrok")
    print("3. Dall'URL, copia SOLO il valore del parametro 'code' (dopo code= e prima di &)\n")

    code = input("Code: ").strip()

    # Exchange code for token
    token_response = requests.post(
        'https://oauth2.googleapis.com/token',
        data={
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'code': code,
            'grant_type': 'authorization_code',
            'redirect_uri': REDIRECT_URI
        }
    )

    if token_response.status_code != 200:
        print(f"❌ Error: {token_response.text}")
        return

    tokens = token_response.json()

    token_data = {
        'token': tokens.get('access_token'),
        'refresh_token': tokens.get('refresh_token'),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scopes': SCOPES
    }

    with open('token.json', 'w') as f:
        json.dump(token_data, f, indent=2)

    print("\n✅ Token saved to token.json")
    print(f"Scopes: {SCOPES}")

if __name__ == '__main__':
    main()

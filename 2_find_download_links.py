"""
Step 2: Find HubSpot report download links from Gmail.

Reads emails from Gmail, extracts download links, and saves them to a JSON file.

Requires:
- GOOGLE_TOKEN_JSON: Google OAuth token (with Gmail scope)

Usage:
    python 2_find_download_links.py
"""

import os
import json
import re
import base64
from datetime import datetime
from pathlib import Path
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Configuration
PROCESSED_LABEL = "Automation/HubSpot-Processed"
OUTPUT_FILE = Path(__file__).parent / "download_links.json"

# All reports to find (report name -> sheet name)
REPORTS = {
    "Time - Create Deal to KYC - Inbound Scorecard": "Time Create Deal to KYC",
    "Qualified Automated - Inbound Scorecard": "Qualified Automated",
    "Click Time - KYC to Click - Inbound Scorecard": "Click Time KYC to Click",
    "QualSales - Inbound Scorecard": "QualSales",
    "Won Sales - Inbound Scorecard": "Won Sales",
    "Days Sales - Inbound Scorecard": "Days Sales",
    "Cohort Qualified - Inbound Scorecard": "Cohort Qualified",
    "NBM - Inbound Scorecard": "NBM",
    "NEW Inbound Automated - Inbound Scorecard": "NEW Inbound Automated",
    "Cohort KYC Automated - Inbound Scorecard": "Cohort KYC Automated",
    "Cohort OnbComplt Automated - Inbound Scorecard": "Cohort OnbComplt Automated",
    "KYC Automated - Inbound Scorecard": "KYC Automated",
    "CTR KYC Email Performance - Inbound Scorecard": "CTR KYC Email Performance",
    "Cohort CTR KYC Email Performance - Inbound Scorecard": "Cohort CTR KYC Email Perf",
    "Cohort Won Automated - Inbound Scorecard": "Cohort Won Automated",
    "Won Automated - Inbound Scorecard": "Won Automated",
    "Email Metrics - Inbound Scorecard": "Email Metrics",
    "SCohort - Sales - Inbound Scorecard": "SCohort Sales",
    "SQL Inbound - Sales - Inbound Scorecard": "SQL Inbound Sales",
    "TTL Automated - Inbound Scorecard": "TTL Automated",
}


def get_gmail_service():
    """Initialize Gmail API service."""
    token_json = os.environ.get("GOOGLE_TOKEN_JSON")

    if token_json:
        token_data = json.loads(token_json)
    else:
        token_file = Path(__file__).parent / "token.json"
        if not token_file.exists():
            raise FileNotFoundError("No token.json found and GOOGLE_TOKEN_JSON not set")
        with open(token_file, "r") as f:
            token_data = json.load(f)

    creds = Credentials(
        token=token_data["token"],
        refresh_token=token_data["refresh_token"],
        token_uri=token_data["token_uri"],
        client_id=token_data["client_id"],
        client_secret=token_data["client_secret"],
        scopes=token_data["scopes"],
    )

    return build("gmail", "v1", credentials=creds)


def get_or_create_label(service, label_name):
    """Get label ID, create if doesn't exist."""
    results = service.users().labels().list(userId="me").execute()
    labels = results.get("labels", [])

    for label in labels:
        if label["name"] == label_name:
            return label["id"]

    # Create label
    label_body = {
        "name": label_name,
        "labelListVisibility": "labelShow",
        "messageListVisibility": "show",
    }
    created = service.users().labels().create(userId="me", body=label_body).execute()
    print(f"Created label: {label_name}")
    return created["id"]


def find_report_email(service, report_name):
    """Find unprocessed HubSpot report email from last 24h."""
    subject = f'Your Report export "{report_name}" has been delivered'
    query = f'subject:"{subject}" -label:{PROCESSED_LABEL} newer_than:1d'

    results = (
        service.users()
        .messages()
        .list(userId="me", q=query, maxResults=1)
        .execute()
    )

    messages = results.get("messages", [])
    return messages[0]["id"] if messages else None


def get_email_body(service, message_id):
    """Get email body (HTML or plain text)."""
    message = (
        service.users()
        .messages()
        .get(userId="me", id=message_id, format="full")
        .execute()
    )

    payload = message.get("payload", {})
    body_data = None

    if "parts" in payload:
        for part in payload["parts"]:
            if part.get("mimeType") == "text/html":
                body_data = part.get("body", {}).get("data")
                break
            elif part.get("mimeType") == "text/plain" and not body_data:
                body_data = part.get("body", {}).get("data")
    else:
        body_data = payload.get("body", {}).get("data")

    if body_data:
        return base64.urlsafe_b64decode(body_data).decode("utf-8")

    return None


def extract_cta_link(email_body):
    """Extract HubSpot CTA link from email body."""
    pattern = r'href="(https://app-eu1\.hubspot\.com/api/notification-station/general/v1/notifications/cta/[^"]+)"'

    match = re.search(pattern, email_body)
    if match:
        link = match.group(1)
        return link.replace("&amp;", "&")

    return None


def main():
    print("=" * 60)
    print("STEP 2: Find Download Links from Gmail")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Initialize Gmail
    print("\nConnecting to Gmail...")
    try:
        service = get_gmail_service()
    except Exception as e:
        print(f"ERROR: Failed to connect to Gmail: {e}")
        return False

    label_id = get_or_create_label(service, PROCESSED_LABEL)

    # Find all report emails
    print(f"\nSearching for {len(REPORTS)} report emails...\n")

    download_links = {}
    results = {"found": 0, "missing": 0}

    for report_name, sheet_name in REPORTS.items():
        print(f"  {report_name}...", end=" ")

        message_id = find_report_email(service, report_name)

        if not message_id:
            print("NOT FOUND")
            results["missing"] += 1
            continue

        # Get email body
        email_body = get_email_body(service, message_id)
        if not email_body:
            print("NO BODY")
            results["missing"] += 1
            continue

        # Extract CTA link
        cta_link = extract_cta_link(email_body)
        if not cta_link:
            print("NO LINK")
            results["missing"] += 1
            continue

        download_links[report_name] = {
            "sheet_name": sheet_name,
            "message_id": message_id,
            "cta_link": cta_link,
            "label_id": label_id,
        }

        print("OK")
        results["found"] += 1

    # Save to JSON
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "reports": download_links,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output_data, f, indent=2)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Found:   {results['found']}")
    print(f"  Missing: {results['missing']}")
    print(f"\nSaved to: {OUTPUT_FILE}")

    if results["found"] > 0:
        return True
    else:
        print("\nERROR: No download links found!")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

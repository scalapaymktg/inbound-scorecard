"""
MVP: Fetch HubSpot report from Gmail, process in memory, mark as processed.
Supports GitHub Actions via environment variables.
"""

import os
import json
import requests
import re
import io
import zipfile
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import base64

# Config - from environment or fallback to hardcoded
GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "stefano.conforti@scalapay.com")
PROCESSED_LABEL = os.environ.get("PROCESSED_LABEL", "Automation/HubSpot-Processed")
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1JoVGtUF3oPCUPye3StilDel_pbnwrO0DP6woA58zGxQ")

# All reports to process (report name -> sheet name)
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
    """Initialize Gmail API service with stored credentials."""
    # Try environment variable first (for GitHub Actions)
    token_json = os.environ.get("GOOGLE_TOKEN_JSON")

    if token_json:
        token_data = json.loads(token_json)
    else:
        with open('token.json', 'r') as f:
            token_data = json.load(f)

    creds = Credentials(
        token=token_data['token'],
        refresh_token=token_data['refresh_token'],
        token_uri=token_data['token_uri'],
        client_id=token_data['client_id'],
        client_secret=token_data['client_secret'],
        scopes=token_data['scopes']
    )

    return build('gmail', 'v1', credentials=creds)

def get_or_create_label(service, label_name):
    """Get label ID, create if doesn't exist."""
    results = service.users().labels().list(userId='me').execute()
    labels = results.get('labels', [])

    for label in labels:
        if label['name'] == label_name:
            return label['id']

    # Create label if not exists
    label_body = {
        'name': label_name,
        'labelListVisibility': 'labelShow',
        'messageListVisibility': 'show'
    }
    created = service.users().labels().create(userId='me', body=label_body).execute()
    print(f"Created label: {label_name}")
    return created['id']

def find_unprocessed_emails(service, report_name, include_processed=False):
    """Find HubSpot report emails not yet processed, from last 24h."""
    subject = f'Your Report export "{report_name}" has been delivered'
    # newer_than:1d = last 24 hours
    if include_processed:
        query = f'subject:"{subject}" newer_than:1d'
    else:
        query = f'subject:"{subject}" -label:{PROCESSED_LABEL} newer_than:1d'

    results = service.users().messages().list(
        userId='me',
        q=query,
        maxResults=1  # Only need the latest one
    ).execute()

    return results.get('messages', [])

def get_email_body(service, message_id):
    """Get email body (HTML or plain text)."""
    message = service.users().messages().get(
        userId='me',
        id=message_id,
        format='full'
    ).execute()

    payload = message.get('payload', {})

    # Try to get HTML body
    body_data = None

    if 'parts' in payload:
        for part in payload['parts']:
            if part.get('mimeType') == 'text/html':
                body_data = part.get('body', {}).get('data')
                break
            elif part.get('mimeType') == 'text/plain' and not body_data:
                body_data = part.get('body', {}).get('data')
    else:
        body_data = payload.get('body', {}).get('data')

    if body_data:
        return base64.urlsafe_b64decode(body_data).decode('utf-8')

    return None

def extract_download_link(email_body):
    """Extract HubSpot download link from email body."""
    # HubSpot notification-station CTA link (new format)
    pattern = r'href="(https://app-eu1\.hubspot\.com/api/notification-station/general/v1/notifications/cta/[^"]+)"'

    match = re.search(pattern, email_body)
    if match:
        link = match.group(1)
        # Clean up HTML entities
        link = link.replace('&amp;', '&')
        return link

    return None

def get_file_id_from_cta_link(cta_url):
    """Extract HubSpot file ID from notification CTA link."""
    # First, get the redirect URL (without following it)
    response = requests.get(cta_url, allow_redirects=False, timeout=30)

    # Check for redirect
    if response.status_code in [301, 302, 303, 307, 308]:
        redirect_url = response.headers.get('Location', '')
    else:
        # If 200, the redirect URL might be in the final URL after JS redirect
        # Look for it in the login redirect parameter
        redirect_url = response.url

    # If we got the login page, extract from the loginRedirectUrl parameter
    if 'login' in redirect_url.lower() or response.status_code == 200:
        # Parse the loginRedirectUrl from the response URL or body
        import urllib.parse
        parsed = urllib.parse.urlparse(response.url)
        params = urllib.parse.parse_qs(parsed.query)
        if 'loginRedirectUrl' in params:
            redirect_url = urllib.parse.unquote(params['loginRedirectUrl'][0])

    # Extract file ID from URL pattern: /files/{FILE_ID}/
    match = re.search(r'/files/(\d+)/', redirect_url)
    if match:
        return match.group(1)

    return None

def get_signed_url_from_hubspot(file_id):
    """Get signed download URL from HubSpot Files API v3."""
    url = f"https://api.hubapi.com/files/v3/files/{file_id}/signed-url"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    return data.get('url')

def download_to_memory(url):
    """Download file to memory (BytesIO), return bytes."""
    print(f"Downloading from: {url[:80]}...")

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    print(f"Downloaded {len(response.content)} bytes")
    return io.BytesIO(response.content)

def extract_csv_from_zip(zip_buffer, use_summary=False):
    """Extract CSV from ZIP in memory, return DataFrame."""
    with zipfile.ZipFile(zip_buffer, 'r') as zf:
        csv_files = [f for f in zf.namelist() if f.endswith('.csv')]

        if not csv_files:
            raise ValueError("No CSV files found in ZIP")

        print(f"  CSV files in ZIP: {csv_files}")

        target_file = None

        if use_summary:
            # Get summary file
            for f in csv_files:
                if 'summary' in f.lower():
                    target_file = f
                    break
        else:
            # Get data file (skip summary)
            for f in csv_files:
                if 'summary' not in f.lower():
                    target_file = f
                    break

        if not target_file:
            target_file = csv_files[0]

        print(f"  Using: {target_file}")

        with zf.open(target_file) as csv_file:
            df = pd.read_csv(csv_file)
            return df

def mark_as_processed(service, message_id, label_id):
    """Add processed label to email."""
    service.users().messages().modify(
        userId='me',
        id=message_id,
        body={'addLabelIds': [label_id]}
    ).execute()
    print(f"Marked message {message_id} as processed")

def get_sheets_service():
    """Initialize Google Sheets API service."""
    # Try environment variable first (for GitHub Actions)
    token_json = os.environ.get("GOOGLE_TOKEN_JSON")

    if token_json:
        token_data = json.loads(token_json)
    else:
        with open('token.json', 'r') as f:
            token_data = json.load(f)

    creds = Credentials(
        token=token_data['token'],
        refresh_token=token_data['refresh_token'],
        token_uri=token_data['token_uri'],
        client_id=token_data['client_id'],
        client_secret=token_data['client_secret'],
        scopes=token_data['scopes']
    )

    return build('sheets', 'v4', credentials=creds)

def ensure_sheet_exists(service, sheet_name):
    """Create sheet if it doesn't exist."""
    try:
        # Get existing sheets
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        existing_sheets = [s['properties']['title'] for s in spreadsheet.get('sheets', [])]

        if sheet_name not in existing_sheets:
            # Create new sheet
            request = {
                'requests': [{
                    'addSheet': {
                        'properties': {'title': sheet_name}
                    }
                }]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body=request
            ).execute()
            print(f"  Created sheet: {sheet_name}")
    except Exception as e:
        print(f"  Warning creating sheet: {e}")

def upload_to_sheets(df, sheet_name="Data"):
    """Upload DataFrame to Google Sheets, replacing existing content."""
    service = get_sheets_service()

    # Ensure sheet exists
    ensure_sheet_exists(service, sheet_name)

    # Convert DataFrame to list of lists (header + data)
    # Handle NaN values by converting to empty string
    df_clean = df.fillna('')
    values = [df_clean.columns.tolist()] + df_clean.values.tolist()

    # Clear existing content
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f'{sheet_name}!A:ZZ'
        ).execute()
    except Exception:
        pass  # Sheet might be empty

    # Write new data
    body = {'values': values}
    result = service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f'{sheet_name}!A1',
        valueInputOption='RAW',
        body=body
    ).execute()

    print(f"  Uploaded {result.get('updatedRows')} rows to {sheet_name}")
    return result

def process_single_report(service, label_id, report_name, sheet_name, include_processed=False):
    """Process a single report: fetch email, download, upload to sheets."""
    print(f"\n{'─' * 40}")
    print(f"Report: {report_name}")
    print(f"Sheet:  {sheet_name}")

    # Find email for this report
    emails = find_unprocessed_emails(service, report_name, include_processed=include_processed)
    if not emails:
        print("  ⏭️  No new email in last 24h")
        return None

    message_id = emails[0]['id']
    print(f"  Found email: {message_id[:12]}...")

    # Get email body
    email_body = get_email_body(service, message_id)
    if not email_body:
        print("  ❌ Could not get email body")
        return None

    # Extract download link
    download_link = extract_download_link(email_body)
    if not download_link:
        print("  ❌ Could not find download link")
        return None

    # Get file ID
    file_id = get_file_id_from_cta_link(download_link)
    if not file_id:
        print("  ❌ Could not extract file ID")
        return None
    print(f"  File ID: {file_id}")

    # Get signed URL and download
    try:
        signed_url = get_signed_url_from_hubspot(file_id)
        zip_buffer = download_to_memory(signed_url)
        # Use summary for all except SCohort Sales
        use_summary = (sheet_name != "SCohort Sales")
        df = extract_csv_from_zip(zip_buffer, use_summary=use_summary)
        print(f"  Downloaded: {len(df)} rows")
    except Exception as e:
        print(f"  ❌ Download error: {e}")
        return None

    # Upload to Google Sheets
    try:
        upload_to_sheets(df, sheet_name)
    except Exception as e:
        print(f"  ❌ Upload error: {e}")
        return None

    # Mark as processed
    mark_as_processed(service, message_id, label_id)
    print(f"  ✅ Done!")

    return df

def main():
    print("=" * 60)
    print("HubSpot Report Fetcher - All Reports")
    print(f"Processing {len(REPORTS)} reports from last 24h")
    print("=" * 60)

    # Initialize
    print("\nConnecting to Gmail...")
    service = get_gmail_service()

    # Get/create label
    label_id = get_or_create_label(service, PROCESSED_LABEL)

    # Process all reports
    results = {"success": 0, "skipped": 0, "failed": 0}

    for report_name, sheet_name in REPORTS.items():
        try:
            df = process_single_report(service, label_id, report_name, sheet_name)
            if df is not None:
                results["success"] += 1
            else:
                results["skipped"] += 1
        except Exception as e:
            print(f"  ❌ Unexpected error: {e}")
            results["failed"] += 1

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"✅ Success: {results['success']}")
    print(f"⏭️  Skipped: {results['skipped']} (no new email)")
    print(f"❌ Failed:  {results['failed']}")

    return results

if __name__ == '__main__':
    main()

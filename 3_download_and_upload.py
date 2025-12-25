"""
Step 3: Download reports and upload to Google Sheets.

Reads download links from JSON, downloads CSVs in memory, uploads to Sheets.

Requires:
- GOOGLE_TOKEN_JSON: Google OAuth token (with Sheets scope)
- HUBSPOT_TOKEN: HubSpot Private App token (PAT)
- SPREADSHEET_ID: Target Google Sheets ID

Usage:
    python 3_download_and_upload.py
"""

import os
import json
import re
import io
import zipfile
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Configuration
HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1JoVGtUF3oPCUPye3StilDel_pbnwrO0DP6woA58zGxQ")
DOWNLOAD_LINKS_FILE = Path(__file__).parent / "download_links.json"


def get_sheets_service():
    """Initialize Google Sheets API service."""
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

    return build("sheets", "v4", credentials=creds)


def get_gmail_service():
    """Initialize Gmail API service for marking emails as processed."""
    token_json = os.environ.get("GOOGLE_TOKEN_JSON")

    if token_json:
        token_data = json.loads(token_json)
    else:
        token_file = Path(__file__).parent / "token.json"
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


def get_file_id_from_cta_link(cta_url):
    """Extract HubSpot file ID from notification CTA link."""
    response = requests.get(cta_url, allow_redirects=False, timeout=30)

    if response.status_code in [301, 302, 303, 307, 308]:
        redirect_url = response.headers.get("Location", "")
    else:
        redirect_url = response.url

    # If login page, extract from loginRedirectUrl
    if "login" in redirect_url.lower() or response.status_code == 200:
        import urllib.parse
        parsed = urllib.parse.urlparse(response.url)
        params = urllib.parse.parse_qs(parsed.query)
        if "loginRedirectUrl" in params:
            redirect_url = urllib.parse.unquote(params["loginRedirectUrl"][0])

    # Extract file ID
    match = re.search(r"/files/(\d+)/", redirect_url)
    if match:
        return match.group(1)

    return None


def get_signed_url(file_id):
    """Get signed download URL from HubSpot Files API."""
    url = f"https://api.hubapi.com/files/v3/files/{file_id}/signed-url"
    headers = {"Authorization": f"Bearer {HUBSPOT_TOKEN}"}

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json().get("url")


def download_and_extract_csv(signed_url, use_summary=False):
    """Download ZIP, extract CSV, return DataFrame."""
    response = requests.get(signed_url, timeout=60)
    response.raise_for_status()

    zip_buffer = io.BytesIO(response.content)

    with zipfile.ZipFile(zip_buffer, "r") as zf:
        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]

        if not csv_files:
            raise ValueError("No CSV files found in ZIP")

        # Select target file
        target_file = None
        if use_summary:
            for f in csv_files:
                if "summary" in f.lower():
                    target_file = f
                    break
        else:
            for f in csv_files:
                if "summary" not in f.lower():
                    target_file = f
                    break

        if not target_file:
            target_file = csv_files[0]

        with zf.open(target_file) as csv_file:
            return pd.read_csv(csv_file)


def ensure_sheet_exists(service, sheet_name):
    """Create sheet if it doesn't exist."""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
        existing_sheets = [s["properties"]["title"] for s in spreadsheet.get("sheets", [])]

        if sheet_name not in existing_sheets:
            request = {
                "requests": [{"addSheet": {"properties": {"title": sheet_name}}}]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body=request
            ).execute()
            print(f"    Created sheet: {sheet_name}")
    except Exception as e:
        print(f"    Warning creating sheet: {e}")


def upload_to_sheets(service, df, sheet_name):
    """Upload DataFrame to Google Sheets."""
    ensure_sheet_exists(service, sheet_name)

    # Convert DataFrame
    df_clean = df.fillna("")
    values = [df_clean.columns.tolist()] + df_clean.values.tolist()

    # Clear existing content
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID, range=f"{sheet_name}!A:ZZ"
        ).execute()
    except Exception:
        pass

    # Write new data
    body = {"values": values}
    result = (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body=body,
        )
        .execute()
    )

    return result.get("updatedRows", 0)


def mark_as_processed(gmail_service, message_id, label_id):
    """Add processed label to email."""
    gmail_service.users().messages().modify(
        userId="me", id=message_id, body={"addLabelIds": [label_id]}
    ).execute()


def main():
    print("=" * 60)
    print("STEP 3: Download and Upload to Sheets")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Load download links
    if not DOWNLOAD_LINKS_FILE.exists():
        print(f"\nERROR: {DOWNLOAD_LINKS_FILE} not found!")
        print("Run step 2 first: python 2_find_download_links.py")
        return False

    with open(DOWNLOAD_LINKS_FILE, "r") as f:
        data = json.load(f)

    reports = data.get("reports", {})

    if not reports:
        print("\nERROR: No download links found in file!")
        return False

    print(f"\nFound {len(reports)} reports to process.\n")

    # Initialize services
    try:
        sheets_service = get_sheets_service()
        gmail_service = get_gmail_service()
    except Exception as e:
        print(f"ERROR: Failed to initialize services: {e}")
        return False

    # Process each report
    results = {"success": 0, "failed": 0}

    for report_name, info in reports.items():
        sheet_name = info["sheet_name"]
        cta_link = info["cta_link"]
        message_id = info["message_id"]
        label_id = info["label_id"]

        print(f"  {report_name}")
        print(f"    -> {sheet_name}...", end=" ")

        try:
            # Get file ID from CTA link
            file_id = get_file_id_from_cta_link(cta_link)
            if not file_id:
                print("NO FILE ID")
                results["failed"] += 1
                continue

            # Get signed URL
            signed_url = get_signed_url(file_id)

            # Download and extract CSV
            use_summary = (sheet_name != "SCohort Sales")
            df = download_and_extract_csv(signed_url, use_summary=use_summary)

            # Upload to Sheets
            rows = upload_to_sheets(sheets_service, df, sheet_name)

            # Mark email as processed
            mark_as_processed(gmail_service, message_id, label_id)

            print(f"OK ({rows} rows)")
            results["success"] += 1

        except Exception as e:
            print(f"ERROR: {e}")
            results["failed"] += 1

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Success: {results['success']}")
    print(f"  Failed:  {results['failed']}")

    return results["failed"] == 0


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

"""
Step 1: Trigger export for all 20 HubSpot Inbound Scorecard reports.

Requires:
- HUBSPOT_CSRF_TOKEN: The CSRF token (stable)
- HUBSPOT_API_COOKIE: The hubspotapi session cookie (changes, manual update)

Usage:
    python 1_trigger_exports.py
"""

import os
import requests
import json
import time
from datetime import datetime
from pathlib import Path

# Configuration
PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "").strip() or "26230674"
CSRF_TOKEN = os.environ.get("HUBSPOT_CSRF_TOKEN", "").strip()
HUBSPOTAPI_COOKIE = os.environ.get("HUBSPOT_API_COOKIE", "").strip()

EXPORT_URL = f"https://app-eu1.hubspot.com/api/reporting-platform/v1/report-definition/export?portalId={PORTAL_ID}&clienttimeout=30000"


def load_payloads():
    """Load report payloads from JSON file."""
    payload_file = Path(__file__).parent / "report_payloads.json"

    if not payload_file.exists():
        raise FileNotFoundError(f"Missing {payload_file}. Please add report payloads.")

    with open(payload_file, "r") as f:
        payloads = json.load(f)

    # Remove helper keys
    payloads.pop("_NOTE", None)

    if len(payloads) < 20:
        print(f"WARNING: Only {len(payloads)} payloads found (expected 20)")

    return payloads


def trigger_export(report_name: str, payload: dict) -> bool:
    """Trigger export for a single report."""
    headers = {
        "Content-Type": "application/json",
        "x-hubspot-csrf-hubspotapi": CSRF_TOKEN,
        "Cookie": f"hubspotapi-csrf={CSRF_TOKEN}; hubspotapi={HUBSPOTAPI_COOKIE}"
    }

    try:
        response = requests.post(EXPORT_URL, headers=headers, json=payload, timeout=60)

        if response.status_code == 200:
            return True
        else:
            print(f"    Error: {response.status_code} - {response.text[:100]}")
            return False
    except Exception as e:
        print(f"    Exception: {e}")
        return False


def main():
    print("=" * 60)
    print("STEP 1: Trigger HubSpot Report Exports")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Validate cookie
    if not HUBSPOTAPI_COOKIE:
        print("\nERROR: HUBSPOT_API_COOKIE not set!")
        print("Please set the HUBSPOT_API_COOKIE environment variable.")
        return False

    # Load payloads
    try:
        payloads = load_payloads()
    except FileNotFoundError as e:
        print(f"\nERROR: {e}")
        return False

    print(f"\nFound {len(payloads)} reports to export.\n")

    # Trigger exports
    results = {"success": 0, "failed": 0}

    for i, (report_name, payload) in enumerate(payloads.items(), 1):
        print(f"[{i:2}/{len(payloads)}] {report_name}...", end=" ")

        if trigger_export(report_name, payload):
            print("OK")
            results["success"] += 1
        else:
            print("FAILED")
            results["failed"] += 1

        # Small delay to avoid rate limiting
        if i < len(payloads):
            time.sleep(1.5)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Success: {results['success']}")
    print(f"  Failed:  {results['failed']}")

    if results["failed"] == 0:
        print("\nAll exports triggered! Emails will arrive in ~1-2 minutes.")
        return True
    else:
        print(f"\nWARNING: {results['failed']} exports failed.")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

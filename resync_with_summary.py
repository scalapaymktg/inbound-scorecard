"""
One-time resync: Re-download all reports using summary CSV (except SCohort Sales).
"""

from hubspot_report_fetcher import (
    get_gmail_service, get_or_create_label, process_single_report,
    REPORTS, PROCESSED_LABEL
)

def main():
    print("=" * 60)
    print("RESYNC - Using Summary CSV for all reports")
    print("(except SCohort Sales which uses data CSV)")
    print("=" * 60)

    service = get_gmail_service()
    label_id = get_or_create_label(service, PROCESSED_LABEL)

    results = {"success": 0, "skipped": 0, "failed": 0}

    for report_name, sheet_name in REPORTS.items():
        try:
            # include_processed=True to re-fetch already processed emails
            df = process_single_report(
                service, label_id, report_name, sheet_name,
                include_processed=True
            )
            if df is not None:
                results["success"] += 1
            else:
                results["skipped"] += 1
        except Exception as e:
            print(f"  ❌ Error: {e}")
            results["failed"] += 1

    print("\n" + "=" * 60)
    print("RESYNC COMPLETE")
    print("=" * 60)
    print(f"✅ Success: {results['success']}")
    print(f"⏭️  Skipped: {results['skipped']}")
    print(f"❌ Failed:  {results['failed']}")

if __name__ == '__main__':
    main()

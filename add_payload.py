"""Helper script to add report payloads to report_payloads.json.

Usage:
1. Run: python add_payload.py
2. Paste the JSON payload (from HubSpot DevTools)
3. Press Enter twice to save
4. Repeat for each report

Or import directly:
    from add_payload import add_payload
    add_payload("Report Name", {...payload...})
"""

import json
from pathlib import Path

PAYLOAD_FILE = Path(__file__).parent / "report_payloads.json"


def load_payloads():
    """Load existing payloads or create empty dict."""
    if PAYLOAD_FILE.exists():
        with open(PAYLOAD_FILE, "r") as f:
            data = json.load(f)
            # Remove helper keys
            data.pop("_NOTE", None)
            return data
    return {}


def save_payloads(payloads: dict):
    """Save payloads to JSON file."""
    with open(PAYLOAD_FILE, "w") as f:
        json.dump(payloads, f, indent=2)
    print(f"Saved {len(payloads)} payloads to {PAYLOAD_FILE}")


def add_payload(name: str, payload: dict):
    """Add a single payload to the collection."""
    payloads = load_payloads()
    payloads[name] = payload
    save_payloads(payloads)
    print(f"Added: {name}")


def add_payload_interactive():
    """Interactive mode to add payloads."""
    print("Paste the JSON payload and press Enter twice when done:")
    print("(Type 'quit' to exit)\n")

    lines = []
    empty_count = 0

    while True:
        try:
            line = input()
            if line.lower() == 'quit':
                break
            if line == '':
                empty_count += 1
                if empty_count >= 2:
                    break
            else:
                empty_count = 0
                lines.append(line)
        except EOFError:
            break

    if not lines:
        print("No input received.")
        return

    json_str = '\n'.join(lines)

    try:
        payload = json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")
        return

    # Extract report name from payload
    report_name = payload.get("exportName", "Unknown Report")
    print(f"\nFound report: {report_name}")

    add_payload(report_name, payload)


def list_payloads():
    """List all saved payloads."""
    payloads = load_payloads()
    print(f"\nSaved payloads ({len(payloads)}):")
    for i, name in enumerate(payloads.keys(), 1):
        print(f"  {i}. {name}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "--list":
            list_payloads()
        elif sys.argv[1] == "--help":
            print(__doc__)
    else:
        list_payloads()
        print("\n" + "="*50)
        add_payload_interactive()
        list_payloads()

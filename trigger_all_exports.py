"""Trigger export for all 20 HubSpot Inbound Scorecard reports."""

import os
import requests
import json
import time
from datetime import datetime
from pathlib import Path

# Session credentials - from environment or fallback to hardcoded
PORTAL_ID = os.environ.get("HUBSPOT_PORTAL_ID", "26230674")
CSRF_TOKEN = os.environ.get("HUBSPOT_CSRF_TOKEN", "AAccUfvRZHnwrDLfFKvBBLR-I2494iVocL_XAQMsbC4Io7VMEgP_IjKQ17SppP50CjBlofaxqpMGBcPG8B2nIeFrnrHAUctLcw")

_DEFAULT_COOKIE = """hs_login_email=stefano.conforti@scalapay.com; NPS_61a7bd63_last_seen=1764929061818; laboratory-anonymous-id=anon357f0f578b5a3a209cb5b4ff2e97; _conv_r=s%3Aapp-eu1.hubspot.com*m%3Areferral*t%3A*c%3A; NPS_d5cc225c_last_seen=1764959579576; hubspotapi-csrf=AAccUfvRZHnwrDLfFKvBBLR-I2494iVocL_XAQMsbC4Io7VMEgP_IjKQ17SppP50CjBlofaxqpMGBcPG8B2nIeFrnrHAUctLcw; hubspotapi-strict=AAccUfsDXO-kGjnubZLXMujvEI-e3o7aRC1YB5dpkVO05xk542jDiIsaEtlx4i-lHIAhe8AJH784jW17IhG7-7a8dK4LvA2ZCg; hs_c2l=GNzH2VsiGQAQstEn1rr8V7akHT4-qSc-9excYVB5nuc; csrf.app=AAccUfvRZHnwrDLfFKvBBLR-I2494iVocL_XAQMsbC4Io7VMEgP_IjKQ17SppP50CjBlofaxqpMGBcPG8B2nIeFrnrHAUctLcw; disable-test-pop-up=true; __hs_cookie_cat_pref=1:true_2:true_3:true; hubspotutk=bb3ecbe4ae2a18598acc198a22bdbe6c; __hssrc=1; _gid=GA1.2.759130396.1765815697; _hjSessionUser_204526=eyJpZCI6IjZkNDMyMTNhLWU3ZjQtNWJkYy1iZTZhLWFmNTYzMWU1ODNjMCIsImNyZWF0ZWQiOjE3NjU4MTU2OTcxMDMsImV4aXN0aW5nIjpmYWxzZX0=; _fbp=fb.1.1765815697281.407093631875309340; _hjSessionUser_2049634=eyJpZCI6IjM3ZmYwODcxLTg0MWMtNWQ2MC1hNTQ3LTMwZmQ3YzQ1OTZiMSIsImNyZWF0ZWQiOjE3NjU4MTU4MDE0ODksImV4aXN0aW5nIjp0cnVlfQ==; IR_gbd=hubspot.com; _conv_v=vi%3A1*sc%3A3*cs%3A1765830763*fs%3A1764934157*pv%3A4*exp%3A%7B%7D*seg%3A%7B10031564.1-10034364.1-10034366.1%7D*ps%3A1765815555; _conv_s=sh%3A1765830762517-0.330373482537077*si%3A3*pv%3A1; saw_experiment=true; __hstc=20629287.bb3ecbe4ae2a18598acc198a22bdbe6c.1765815558391.1765815558391.1765830874521.2; __hssc=20629287.1.1765830874521; _ga_LXTM6CQ0XK=GS2.1.s1765830874$o2$g0$t1765830874$j60$l0$h0; _ga=GA1.1.180667605.1765815697; _hjSession_2049634=eyJpZCI6IjEwNzgxYmFmLTZiZjgtNDlmZS05MGFmLTBjZTBhOWMzYzU4YiIsImMiOjE3NjU4MzA4NzUwMDcsInMiOjEsInIiOjEsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MX0=; IR_12893=1765830874908%7Cc-23860%7C1765830874908%7C%7C; IR_PI=1c72cde3-b567-11f0-bee9-bf4ba448566c%7C1765917274908; hubspotapi=AAccUfu4Ek3IWXQ5PA_EazFqo-LrmmdTwKwcFxWP8Eto2sbCSDkY30hvQjRLu4-zTISFH-xA0Y4Uw1MJnj5iAgBiLwNroX7LPdun9IHkwcvPXJv-aig9MS9Yucbn7RU5RAFOAAO0GhClKCYxmDk7DHez8XCBzhH_NbPM3tMqrsN1fLnaIFzjGC6nLUp7qU_anTzEjeS8gxn3V4z_Lju1vF4zHLrZdhRB7MRgoisYO7ny3npzeulyu3dZmRCVRr94m2NlibyUCbxEhgx25FZ3zWAlPEMmZlC9FckYXFCO1Hi8A9tBoEtTTRYB_FGCUGhjdB44ssksY4VXOQn2n-_L0hzhginQAxSndbFWl8UyHY5ejD7mm99gepI4F8uh6XaF2kZAPXzYPc6hidV3Cr0VtSDTojxvwxsOnABy0SU3916vE_l3aFiwdqQxLyXX_qV_lrOEr5gtWbgvZXqC1WF7xJNAFU16tyL249JWrv8of7qrr_LuA5rNQlWlj7tCw1yO5j5uJYaqwHT8mCJ3t0ZbAOm5DaD5zGmlSteqLAh9IssMfbiomDzq-XodWfDveu_Zu8Fbpp--O3fVE5VG8As7Vnfyg918HXNYk0Bz76C9L72lol7S-fOtd_pl3geds9590zOrSxZ9w89ujACVa8qKNtyDezFVH3_7rA3FdDAlAfYWqPlV2uv4CnjHpTzuNpWWUTjsifFi-p9X3qNQgHkvz_Hdy4px6dJOECdZfXqn4Xe5R243O2W23ANMfUPmGk6mdUXAZoFGEmQIm22Yclm_J2767tuyMW3O2pRKoySnmG6lB4QGX82-HlOLeWM5ZVsnuRNrlmer3sCvZjgGtEypfAgZ58x1JexTZNpN9zZbpSo-GPjvt99HhuzbtY_j4txaoQRYDRp9PXqQa1243CYPrskm3a7EHkg88E78u2lyP-cdlirASWs9RlYx4caeeJ1lU3ETo1l3yyfF4Y_r-RZmFIhoNRLeFWNSD1evox77jhCYwcmFOi4cTEOiOD5unssOOdSxSt1l5uCsbdMdfLO-iDYyfqMmgl9xE7cFO1Yk4fheIvnveQRscSGIdv5_JFq8PBiT7Cy0q6H4y2_qytUzR2zmk09AvzfyVf13aMfJdOJPemXsbOc_yWCiShIvRfLJZZUmfuELHEdU1cWMrQwFYLtbLUXkmOXEUl2SVE-eb299EL25W1ipFFyb1EbhacoRMt41GvHkKKKgh4eB7z5RMMem-j9jjufjU674yZbsOSd1MKZ0LutYiiPibuV01wixXrJTMxcGTi-_DHHRsGiXzTC00VIMHmAWHeBvZ_ien1d3NaBCOz5-gomWNgNDqEujtt1Mh1E_IB842V0mWeWdUQ-7RcnR1vm0ZlfRmvMdSog5yW-T67DorgypBUeBPFhR0yiVi3TC6v5ISx_X47pKmHN2zbjjrK4X1eUweCirS8POx069n7hgOwTVyQlM6Whe6xyZWsy7EDBEMaRAHbvfyzGpi3P-0X3_bNOMrYuwaxukW98fgoclcm-kEugJYIJs5gQZNjcKCqnFLicVPHYcqNrDyYwLSOM7nzvvUvkNt7hcRFVZzv24t420rsjLL9Yg8TDcwyM4u9Jq71WGQ-dt1258wwrhZTtVEVPbxtandEHkthHFBKgwnAJL-VMYvxP5s1pNVCcwsd0W045vhWxbfiv_QQMg0sDilcNLdod43GjtkGcEKllCfwmbLXVrj3R934L2n0achxZETImQ7xSazRnighQ8wMH7PEMLwBYuwkBwq21vq4aXKurXigxB49vZc0Fwg6DpOULlmHUHTts2kHZq0CwSPheccs_M5-y6JeA4ELnHJ63jUnvs62qkqFoJpPd2XEw5FGeItmffWt0QfiJcV83WeaXzQFmmaaeIh2k4QarIRhfHCBv-pPRelcIFOjwuf4ho2j1cA17S_maR4-rNFmOd4ffHSy2clj2Iva6xdv_xFjkS7XcKgcjZc2axtLWSrQ6c5UOUo0Ea9IRQzEqCxMMZn9ujJoWD0s8LDHpRBnpS9t7lK8k2zIppJ6qYokY-Dt-67jKFnbQRMM4lvHIYKV4RFRXcJuzvGdlb5P9ypo_4_fbS_X-JsZfoCO0vXdqAKyBycq0pHUKOPPRY2pl_Jc8mCYGHY7Nq_pBp8sBMueP1HhfxKJsYiXk4kfSpGwIRLHniwGPLdft4eBdU7zxEsC7yQOOcq1NbXhAnraSfIjmnzM43HaPj_pDsRGNCJek40MnyW3gF3KQ4qpyyVXHZ2T1hlc3R8Ee9hfOtqutvzBr5SFgwQCdaJJz-3QvRRaUh1gcW1blCK1xPDEcjZMsIPVtBUcObTyHXBu99Q46PdTyev7KYHbj8PhNxowgfwyZ1fbxS0B_dEiij5TgumcFebih1EvFwApOqUzgsdVk8VcEKWrnHx5JxW-XYfQ_z4SknHEv6AdOT5NnzM_b89wbF20prUfyycpBNeEU6-5ClEOXsXh8p1-JNf4p8BSehTHgRw9Zlvah-viN886RFd4O9pcNEbDg0a2ztK6G5enCEsedsdego-yKyKKUW3aF2o6j_NV3_aBiHKEATzkjgDufbSuArQVK4fZVVzYnWzWXsiMp9VNBFJ4dELpu_aUDaAL_vdcwsILLpEJ45b0bny5AhtOuvGAK2nVgKc47S6D21Krm0AYR3MeHi5WNxXQVZMfRolSZYMDUsKn4EuM4Agh-FAo14efpUj0mf3xjJAmRL894pcaEOdzyzLEoqR9Bu-NEn3qTa1H1Oay5fWY1_mtub-kwrCYQOR1iKfDieev_Dz7Ys4ZaeX7IPY892BEvrIz6a5rIu2VtgoLcxwvkwPqUpFKmx2OV3fRdLgxoNSnhMTgkoCnMuhuwfdKhV_oTR3YRKPiLmXEu0D78yTi5enRv52JjRzTD4IDQDzoRZV05-dozhOykbzlv6SE8N7hnjkSShREci6PoETRlkpBuFTOl6pKO-R66GToI6LhDbI6nAGdbG8G3hHOe5IGNLN_CRnLjFyZs5WRk9e8ErZrWjB3aPr0UB9pCDfDNeLHRitHyxSWs0Gxo_3QeQFB1LMxjEZQzfPDxOCsxYPiEYnxTAsSfWLOLaLseBGzpJR9Fv4BjdC9FDvL6U2Wt8eUrqOG6JzRAWF0iyGKyHZ10Sere_WRCQrP1ZsRQ0EaI0gdNq6W4XNiMcnPW6DCeuNnCTrHyu_J0m7BwpvrOXMK2Iy2RH4BSRTaDx7VulSNkwjdgHmZeyFz2nPmPm7suc0PoMHrXLil_z_0WvlHLpBhAHe7N2ayzpGmko8r0rFj5yJ2idpmgNxv4ASLNfUvTcOzSks8Pse1C1uSSpO0xonbsgXHxcvtjjSNgxyWmsdNvQwPgg-CUYV5sMTUXaWMJlBFa6lIKUhJQhKw3QEMFuLRUJ4B4IFSRFKvJqI8ls6bFLs1RySJNFUWpp6F0gGphKPDDeRCpONgzjZ0WUN0ZUgeqzhqmsReUXmly5Pgzxt2XazXLD6NnPAOw-xPDFzKBCnPmzqcB_AOM_V_AOY6HRFcCgUbCE3jA7SSREicTaI4XzX0xLo3HBb4HhkEMsUMlnC4zmiAbtrwLJef0Fg-Xld7XnSsyDKDlspBf5SieiAUxcY2yYbuuHEgv8HspsTSd7-qjpHKcP9KkJyJZ2zXvxScXTlnVdRFFPPeTaBlfSvel20T5Ha9OSq5CdmIYQtIPuA9TliXjugmEW3MzbO9yNuirR3A1PrfHRS51bShsGtwy1pt-lgKFTdInMkXSytlA; __cf_bm=avfUAsjTiKYuB1Mj6xJZoboBrgFufR0j.UtaRLRc15w-1765831126-1.0.1.1-ynR1vwBFtF59W1dc7YD3Ulod4EDt6UIJEotSpmeAG.ZqHHFH0iZzlhdnfcPgh86wWIB7ys1mw8IpC3E5iR1EFGEigno8AFRJ4Gb7_nOaSyY; _cfuvid=R5R8I3jglOUgAdg818z4nVPV_uklMl3Em8RlH8U.wMU-1765831126587-0.0.1.1-604800000"""

COOKIE = os.environ.get("HUBSPOT_COOKIE", _DEFAULT_COOKIE)

# All 20 report payloads (embedded to avoid separate file dependency)
REPORT_PAYLOADS = {
    "SCohort - Sales - Inbound Scorecard": {"exportName": "SCohort - Sales - Inbound Scorecard"},
    "Email Metrics - Inbound Scorecard": {"exportName": "Email Metrics - Inbound Scorecard"},
    "CTR KYC Email Performance - Inbound Scorecard": {"exportName": "CTR KYC Email Performance - Inbound Scorecard"},
    "Cohort CTR KYC Email Performance - Inbound Scorecard": {"exportName": "Cohort CTR KYC Email Performance - Inbound Scorecard"},
    "TTL Automated - Inbound Scorecard": {"exportName": "TTL Automated - Inbound Scorecard"},
    "Cohort Won Automated - Inbound Scorecard": {"exportName": "Cohort Won Automated - Inbound Scorecard"},
    "Cohort OnbComplt Automated - Inbound Scorecard": {"exportName": "Cohort OnbComplt Automated - Inbound Scorecard"},
    "Cohort KYC Automated - Inbound Scorecard": {"exportName": "Cohort KYC Automated - Inbound Scorecard"},
    "Cohort Qualified - Inbound Scorecard": {"exportName": "Cohort Qualified - Inbound Scorecard"},
    "Won Automated - Inbound Scorecard": {"exportName": "Won Automated - Inbound Scorecard"},
    "KYC Automated - Inbound Scorecard": {"exportName": "KYC Automated - Inbound Scorecard"},
    "NEW Inbound Automated - Inbound Scorecard": {"exportName": "NEW Inbound Automated - Inbound Scorecard"},
    "Won Sales - Inbound Scorecard": {"exportName": "Won Sales - Inbound Scorecard"},
    "QualSales - Inbound Scorecard": {"exportName": "QualSales - Inbound Scorecard"},
    "NBM - Inbound Scorecard": {"exportName": "NBM - Inbound Scorecard"},
    "Days Sales - Inbound Scorecard": {"exportName": "Days Sales - Inbound Scorecard"},
    "Qualified Automated - Inbound Scorecard": {"exportName": "Qualified Automated - Inbound Scorecard"},
    "Time - Create Deal to KYC - Inbound Scorecard": {"exportName": "Time - Create Deal to KYC - Inbound Scorecard"},
    "Click Time - KYC to Click - Inbound Scorecard": {"exportName": "Click Time - KYC to Click - Inbound Scorecard"},
    "SQL Inbound - Sales - Inbound Scorecard": {"exportName": "SQL Inbound - Sales - Inbound Scorecard"},
}


def load_payloads():
    """Load all report payloads from JSON file or use embedded placeholders."""
    payload_file = Path(__file__).parent / "report_payloads.json"

    if payload_file.exists():
        try:
            with open(payload_file, "r") as f:
                payloads = json.load(f)
                # Remove helper keys
                payloads.pop("_NOTE", None)
                if len(payloads) >= 20:
                    return payloads
        except Exception:
            pass

    # Fallback to embedded payloads (placeholders - need full payloads from add_payload.py)
    print("WARNING: Using placeholder payloads. Run add_payload.py to populate full payloads.")
    return REPORT_PAYLOADS


def trigger_export(report_name: str, payload: dict) -> bool:
    """Trigger export for a single report."""
    url = f"https://app-eu1.hubspot.com/api/reporting-platform/v1/report-definition/export?portalId={PORTAL_ID}&clienttimeout=30000"

    headers = {
        "Content-Type": "application/json",
        "x-hubspot-csrf-hubspotapi": CSRF_TOKEN,
        "Cookie": COOKIE,
        "Accept": "application/json",
        "Origin": "https://app-eu1.hubspot.com",
        "Referer": "https://app-eu1.hubspot.com/reports-list/26230674"
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)

        if response.status_code == 200:
            print(f"  [OK] {report_name}")
            return True
        else:
            print(f"  [FAIL] {report_name} - Status {response.status_code}")
            return False
    except Exception as e:
        print(f"  [ERROR] {report_name} - {e}")
        return False


def trigger_all_exports():
    """Trigger export for all 20 reports."""
    print(f"\n{'='*60}")
    print(f"HubSpot Report Export Trigger")
    print(f"Time: {datetime.now().isoformat()}")
    print(f"{'='*60}\n")

    payloads = load_payloads()

    print(f"Found {len(payloads)} reports to export.\n")

    success_count = 0
    fail_count = 0

    for i, (report_name, payload) in enumerate(payloads.items(), 1):
        print(f"[{i}/{len(payloads)}] Triggering: {report_name}")

        if trigger_export(report_name, payload):
            success_count += 1
        else:
            fail_count += 1

        # Small delay between requests to avoid rate limiting
        if i < len(payloads):
            time.sleep(2)

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"  Total:   {len(payloads)}")
    print(f"  Success: {success_count}")
    print(f"  Failed:  {fail_count}")
    print(f"{'='*60}\n")

    if fail_count == 0:
        print("All reports triggered successfully!")
        print("Check your email for the CSV exports.")
    else:
        print(f"Warning: {fail_count} reports failed to trigger.")

    return fail_count == 0


def test_session():
    """Quick check if session is still valid."""
    url = f"https://app-eu1.hubspot.com/api/reporting-platform/v1/reports?portalId={PORTAL_ID}"

    headers = {
        "x-hubspot-csrf-hubspotapi": CSRF_TOKEN,
        "Cookie": COOKIE,
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 200:
        print("Session is VALID")
        return True
    else:
        print("Session is EXPIRED - please update COOKIE and CSRF_TOKEN")
        return False


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        # Just test session validity
        test_session()
    else:
        # First check session, then trigger all exports
        print("Checking session validity...")
        if test_session():
            print()
            trigger_all_exports()
        else:
            print("\nPlease update the session credentials before running exports.")
            sys.exit(1)

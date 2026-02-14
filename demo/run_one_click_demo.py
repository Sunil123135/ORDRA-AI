"""
One-click demo: generate PO PDF, send email via Graph, run ORDRA job, poll until done.

Env vars:
  O365_TENANT_ID, O365_CLIENT_ID, O365_CLIENT_SECRET
  O365_MAILBOX (mailbox ORDRA reads, e.g. orders@company.com)
  DEMO_SENDER_UPN (sender mailbox; needs Mail.Send for app-only send)
  DEMO_RECIPIENT (e.g. orders@company.com)
  ORDRA_API_BASE (default http://localhost:8000)

Install demo deps: uv sync --extra demo  (or pip install reportlab)
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

from demo.generate_po_pdf import build_po_pdf

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


def get_token() -> str:
    tenant = os.environ["O365_TENANT_ID"]
    cid = os.environ["O365_CLIENT_ID"]
    secret = os.environ["O365_CLIENT_SECRET"]
    url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"
    data = {
        "client_id": cid,
        "client_secret": secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def send_mail_with_attachment(
    token: str,
    sender_upn: str,
    recipient: str,
    subject: str,
    body_text: str,
    attachment_path: str,
) -> None:
    import base64

    with open(attachment_path, "rb") as f:
        content_bytes = base64.b64encode(f.read()).decode("utf-8")

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": body_text},
            "toRecipients": [{"emailAddress": {"address": recipient}}],
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": os.path.basename(attachment_path),
                    "contentType": "application/pdf",
                    "contentBytes": content_bytes,
                }
            ],
        },
        "saveToSentItems": "true",
    }

    url = f"{GRAPH_BASE}/users/{sender_upn}/sendMail"
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=30,
    )
    r.raise_for_status()


def ordra_create_and_run_job(api_base: str, mailbox_query: dict) -> str:
    # Create job via /intake (accepts mailbox_query)
    create = requests.post(
        f"{api_base}/intake",
        json={"mailbox_query": mailbox_query},
        timeout=30,
    )
    create.raise_for_status()
    job = create.json()
    job_id = job["job_id"]

    # Run
    run = requests.post(f"{api_base}/jobs/{job_id}/run", timeout=30)
    run.raise_for_status()
    return job_id


def ordra_poll_job(api_base: str, job_id: str, timeout_s: int = 180) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        r = requests.get(f"{api_base}/jobs/{job_id}", timeout=30)
        r.raise_for_status()
        j = r.json()
        status = j.get("status")
        outputs = j.get("outputs") or {}
        decision = outputs.get("decision") or j.get("decision") or {}
        sap_so = outputs.get("sap_order_number") or j.get("sap_order_number")

        if status in ("COMPLETED", "FAILED"):
            return j
        if decision.get("action") == "AUTO_POST" and sap_so:
            return j

        time.sleep(2)

    raise RuntimeError("Timeout waiting for job completion")


def main() -> None:
    po_number = "4500-ORDRA-DEMO"
    sold_to = "1040402"
    ship_to = "1040402"
    pdf_name = f"PO_{po_number}_{sold_to}.pdf"
    out_path = Path(__file__).resolve().parent / pdf_name

    build_po_pdf(str(out_path), po_number=po_number, sold_to=sold_to, ship_to=ship_to)

    subject = f"PO {po_number} | Sold-To {sold_to} | Ship-To {ship_to}"
    body = f"""Hi Team,

Please find attached Purchase Order for processing.

PO Number: {po_number}
Sold-To: {sold_to}
Ship-To: {ship_to}
Requested Delivery Date: 2026-02-15

Regards,
Sunil Lalwani
QuidelOrtho
sunil.lalwani@quidelortho.com
"""

    token = get_token()
    sender = os.environ["DEMO_SENDER_UPN"]
    recipient = os.environ["DEMO_RECIPIENT"]
    send_mail_with_attachment(token, sender, recipient, subject, body, str(out_path))
    print("✅ Demo email sent with PO PDF.")

    received_after = (datetime.utcnow() - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    mailbox_query = {
        "folder": "Inbox",
        "from_addresses": ["sunil.lalwani@quidelortho.com"],
        "subject_contains": ["PO", po_number],
        "has_attachments": True,
        "received_after_iso": received_after,
        "max_results": 5,
    }

    api_base = os.environ.get("ORDRA_API_BASE", "http://localhost:8000")
    job_id = ordra_create_and_run_job(api_base, mailbox_query)
    print(f"🚀 ORDRA job started: {job_id}")

    result = ordra_poll_job(api_base, job_id)
    print("✅ ORDRA job completed.")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

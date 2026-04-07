# worker/resend_client.py
from __future__ import annotations

import base64
import json
import os
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import List, Optional, Dict, Any


@dataclass
class ResendAttachment:
    filename: str
    content_type: str
    content_bytes: bytes

    def as_payload(self) -> Dict[str, Any]:
        return {
            "filename": self.filename,
            "content": base64.b64encode(self.content_bytes).decode("utf-8"),
            "content_type": self.content_type,
        }


def send_email_resend(
    *,
    from_email: str,
    to_emails: List[str],
    subject: str,
    html: str,
    attachments: Optional[List[ResendAttachment]] = None,
) -> Dict[str, Any]:
    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        raise RuntimeError("RESEND_API_KEY no configurada")

    if not from_email:
        raise RuntimeError("from_email vacío")

    if not to_emails or not all(isinstance(x, str) and "@" in x for x in to_emails):
        raise RuntimeError("to_emails inválido")

    payload: Dict[str, Any] = {
        "from": from_email.strip("<>").strip(),
        "to": [x.strip() for x in to_emails],
        "subject": subject,
        "html": html,
    }

    if attachments:
        payload["attachments"] = [a.as_payload() for a in attachments]

    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": "Bearer " + api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "turnera/0.1",
    }

    req = urllib.request.Request(
        "https://api.resend.com/emails",
        data=data,
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            return {"status": resp.status, "body": body}
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        raise RuntimeError(f"Resend HTTP {e.code}: {body}") from e
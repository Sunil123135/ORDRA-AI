from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


class O365Error(Exception):
    pass


@dataclass
class O365Config:
    tenant_id: str
    client_id: str
    client_secret: str
    mailbox: str  # user or shared mailbox UPN/email, e.g. orders@company.com
    graph_base: str = "https://graph.microsoft.com/v1.0"
    token_url_base: str = "https://login.microsoftonline.com"
    timeout_seconds: int = 30
    top: int = 25


@dataclass
class MessageCandidate:
    id: str
    internet_message_id: str
    conversation_id: str
    subject: str
    sender: str
    received_at: str
    has_attachments: bool
    body_preview: str


@dataclass
class AttachmentFile:
    name: str
    content_type: str
    bytes_data: bytes


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def load_config_from_env() -> O365Config:
    tenant = _env("O365_TENANT_ID")
    cid = _env("O365_CLIENT_ID")
    secret = _env("O365_CLIENT_SECRET")
    mailbox = _env("O365_MAILBOX")

    if not all([tenant, cid, secret, mailbox]):
        missing = [k for k in ["O365_TENANT_ID", "O365_CLIENT_ID", "O365_CLIENT_SECRET", "O365_MAILBOX"] if not _env(k)]
        raise O365Error(f"Missing env vars: {', '.join(missing)}")

    return O365Config(
        tenant_id=tenant,
        client_id=cid,
        client_secret=secret,
        mailbox=mailbox,
    )


class O365Client:
    """
    Microsoft Graph connector (App-only / Client Credentials).
    Supports:
      - search messages (deterministic filters)
      - fetch message (headers/body)
      - list/download attachments (fileAttachment)
      - ensure folders (Processed/Failed/Needs CS)
      - move messages
    """

    def __init__(self, cfg: Optional[O365Config] = None, session: Optional[requests.Session] = None):
        self.cfg = cfg or load_config_from_env()
        self.sess = session or requests.Session()
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._folder_cache: Dict[str, str] = {}

    # ---------------------------
    # Auth
    # ---------------------------
    def _get_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expiry - 60:
            return self._token

        token_url = f"{self.cfg.token_url_base}/{self.cfg.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.cfg.client_id,
            "client_secret": self.cfg.client_secret,
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }
        r = self.sess.post(token_url, data=data, timeout=self.cfg.timeout_seconds)
        if r.status_code != 200:
            raise O365Error(f"Token error {r.status_code}: {r.text[:300]}")
        j = r.json()
        self._token = j["access_token"]
        expires_in = int(j.get("expires_in", 3599))
        self._token_expiry = now + expires_in
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_token()}"}

    def _url(self, path: str) -> str:
        if path.startswith("http"):
            return path
        return f"{self.cfg.graph_base}{path}"

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        r = self.sess.get(self._url(path), headers=self._headers(), params=params, timeout=self.cfg.timeout_seconds)
        if r.status_code >= 300:
            raise O365Error(f"GET {path} failed {r.status_code}: {r.text[:300]}")
        return r.json()

    def _post(self, path: str, json_body: Dict[str, Any]) -> Dict[str, Any]:
        r = self.sess.post(self._url(path), headers=self._headers(), json=json_body, timeout=self.cfg.timeout_seconds)
        if r.status_code >= 300:
            raise O365Error(f"POST {path} failed {r.status_code}: {r.text[:300]}")
        return r.json()

    # ---------------------------
    # Message Search
    # ---------------------------
    def search_messages(
        self,
        *,
        folder: str = "Inbox",
        from_addresses: Optional[List[str]] = None,
        subject_contains: Optional[List[str]] = None,
        has_attachments: Optional[bool] = True,
        received_after_iso: Optional[str] = None,  # e.g. 2026-02-01T00:00:00Z
        max_results: int = 25,
    ) -> List[MessageCandidate]:
        """
        Deterministic mailbox search using $filter.
        Note: Graph restricts mixing $search and complex $filter; we avoid $search here.
        """
        from_addresses = from_addresses or []
        subject_contains = subject_contains or []

        # Build filter
        filters = []
        if has_attachments is not None:
            filters.append(f"hasAttachments eq {str(has_attachments).lower()}")
        if received_after_iso:
            filters.append(f"receivedDateTime ge {received_after_iso}")
        if from_addresses:
            # OR chain
            ors = " or ".join([f"from/emailAddress/address eq '{a}'" for a in from_addresses])
            filters.append(f"({ors})")
        # subject contains isn't directly supported in $filter for all tenants consistently.
        # We'll do subject filter client-side (safe) after fetch.
        filter_expr = " and ".join(filters) if filters else None

        select_fields = [
            "id",
            "internetMessageId",
            "conversationId",
            "subject",
            "from",
            "receivedDateTime",
            "hasAttachments",
            "bodyPreview",
        ]
        params = {
            "$select": ",".join(select_fields),
            "$top": min(max_results, self.cfg.top),
            "$orderby": "receivedDateTime desc",
        }
        if filter_expr:
            params["$filter"] = filter_expr

        # Folder endpoint
        folder_id = self.get_folder_id(folder)
        path = f"/users/{self.cfg.mailbox}/mailFolders/{folder_id}/messages"

        out: List[MessageCandidate] = []
        page = self._get(path, params=params)
        out.extend(self._parse_candidates(page.get("value", []), subject_contains))

        # Pagination
        while page.get("@odata.nextLink") and len(out) < max_results:
            page = self._get(page["@odata.nextLink"])
            out.extend(self._parse_candidates(page.get("value", []), subject_contains))
            if len(out) >= max_results:
                break

        return out[:max_results]

    def _parse_candidates(self, rows: List[Dict[str, Any]], subject_contains: List[str]) -> List[MessageCandidate]:
        out = []
        subj_terms = [s.lower() for s in (subject_contains or []) if s.strip()]
        for m in rows:
            subj = (m.get("subject") or "")
            if subj_terms and not any(t in subj.lower() for t in subj_terms):
                continue

            sender = ""
            frm = m.get("from") or {}
            sender = ((frm.get("emailAddress") or {}).get("address")) or ""

            out.append(
                MessageCandidate(
                    id=m.get("id", ""),
                    internet_message_id=m.get("internetMessageId", "") or "",
                    conversation_id=m.get("conversationId", "") or "",
                    subject=subj,
                    sender=sender,
                    received_at=m.get("receivedDateTime", "") or "",
                    has_attachments=bool(m.get("hasAttachments")),
                    body_preview=m.get("bodyPreview", "") or "",
                )
            )
        return out

    # ---------------------------
    # Message Fetch
    # ---------------------------
    def fetch_message(self, message_id: str) -> Dict[str, Any]:
        """
        Returns full message: headers, body, etc.
        Body is usually HTML; convert to text downstream.
        """
        select_fields = [
            "id",
            "internetMessageId",
            "conversationId",
            "subject",
            "from",
            "toRecipients",
            "ccRecipients",
            "receivedDateTime",
            "hasAttachments",
            "body",
            "bodyPreview",
        ]
        path = f"/users/{self.cfg.mailbox}/messages/{message_id}"
        return self._get(path, params={"$select": ",".join(select_fields)})

    # ---------------------------
    # Attachments
    # ---------------------------
    def list_attachments(self, message_id: str) -> List[Dict[str, Any]]:
        path = f"/users/{self.cfg.mailbox}/messages/{message_id}/attachments"
        return self._get(path).get("value", [])

    def download_file_attachments(
        self,
        message_id: str,
        *,
        allowed_content_types: Optional[List[str]] = None,
        allowed_name_exts: Optional[List[str]] = None,
        max_files: int = 10,
        max_total_bytes: int = 25 * 1024 * 1024,
    ) -> List[AttachmentFile]:
        """
        Graph returns fileAttachment content in 'contentBytes' (base64) in attachment list.
        """
        allowed_content_types = allowed_content_types or [
            "application/pdf",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-excel",
            "text/csv",
        ]
        allowed_name_exts = allowed_name_exts or [".pdf", ".xlsx", ".xls", ".csv"]

        att = self.list_attachments(message_id)
        files: List[AttachmentFile] = []
        total = 0

        for a in att:
            if len(files) >= max_files:
                break
            if a.get("@odata.type") != "#microsoft.graph.fileAttachment":
                continue

            name = (a.get("name") or "").strip()
            ctype = (a.get("contentType") or "").strip()

            if ctype not in allowed_content_types and not any(name.lower().endswith(ext) for ext in allowed_name_exts):
                continue

            b64 = a.get("contentBytes")
            if not b64:
                continue
            try:
                data = base64.b64decode(b64)
            except Exception:
                continue

            total += len(data)
            if total > max_total_bytes:
                break

            files.append(AttachmentFile(name=name, content_type=ctype, bytes_data=data))

        return files

    # ---------------------------
    # Folder ops / Routing
    # ---------------------------
    def get_folder_id(self, display_name: str) -> str:
        """
        Resolve folder by displayName; caches result.
        For Inbox use well-known name "Inbox" (exists).
        """
        key = display_name.lower()
        if key in self._folder_cache:
            return self._folder_cache[key]

        path = f"/users/{self.cfg.mailbox}/mailFolders"
        page = self._get(path, params={"$top": 200, "$select": "id,displayName"})
        for f in page.get("value", []):
            if (f.get("displayName") or "").lower() == key:
                fid = f["id"]
                self._folder_cache[key] = fid
                return fid

        fid = self.ensure_folder(display_name)
        self._folder_cache[key] = fid
        return fid

    def ensure_folder(self, display_name: str) -> str:
        """
        Creates folder if missing under MsgFolderRoot.
        """
        path = f"/users/{self.cfg.mailbox}/mailFolders"
        page = self._get(path, params={"$top": 200, "$select": "id,displayName"})
        for f in page.get("value", []):
            if (f.get("displayName") or "").lower() == display_name.lower():
                return f["id"]

        create_path = f"/users/{self.cfg.mailbox}/mailFolders"
        j = self._post(create_path, {"displayName": display_name})
        return j["id"]

    def move_message(self, message_id: str, destination_folder_name: str) -> Dict[str, Any]:
        dest_id = self.get_folder_id(destination_folder_name)
        path = f"/users/{self.cfg.mailbox}/messages/{message_id}/move"
        return self._post(path, {"destinationId": dest_id})

    def route_message(self, message_id: str, route: str) -> Dict[str, Any]:
        """
        route: "Processed" | "Failed" | "Needs CS"
        """
        if route not in {"Processed", "Failed", "Needs CS"}:
            raise O365Error(f"Invalid route: {route}")
        self.ensure_folder(route)
        return self.move_message(message_id, route)

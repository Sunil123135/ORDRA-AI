"""SAP client: stub (deterministic SO 0090012345) or ECC via RFC (pyrfc)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class SapResult:
    ok: bool
    sales_order: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class SapClient:
    """
    SAP client wrapper.
    Modes:
      - stub: deterministic demo order creation (SO 0090012345)
      - ecc: real SAP ECC integration via RFC (pyrfc)
    """

    def __init__(self) -> None:
        self.mode = os.getenv("SAP_MODE", "stub").strip().lower()

    def create_sales_order(self, order_payload: Dict[str, Any]) -> SapResult:
        if self.mode == "stub":
            return self._create_sales_order_stub(order_payload)
        if self.mode == "ecc":
            return self._create_sales_order_ecc(order_payload)
        return SapResult(ok=False, error=f"Unknown SAP_MODE={self.mode}")

    # -------------------------
    # STUB MODE
    # -------------------------
    def _create_sales_order_stub(self, order_payload: Dict[str, Any]) -> SapResult:
        """
        Always returns a realistic sales order number for demo,
        and echoes back key inputs for audit.
        """
        return SapResult(
            ok=True,
            sales_order="0090012345",
            raw={
                "mode": "stub",
                "echo": {
                    "sold_to": order_payload.get("sold_to"),
                    "ship_to": order_payload.get("ship_to"),
                    "po_number": order_payload.get("po_number"),
                    "items": order_payload.get("items", []),
                },
            },
        )

    # -------------------------
    # ECC MODE (RFC)
    # -------------------------
    def _create_sales_order_ecc(self, order_payload: Dict[str, Any]) -> SapResult:
        """
        Real ECC call via pyrfc.
        You must install SAP NW RFC SDK + pyrfc and provide connection params.
        """
        try:
            from pyrfc import Connection
        except Exception as e:
            return SapResult(ok=False, error=f"pyrfc not available: {e}")

        conn_params = {
            "user": os.getenv("SAP_USER", ""),
            "passwd": os.getenv("SAP_PASS", ""),
            "ashost": os.getenv("SAP_ASHOST", ""),
            "sysnr": os.getenv("SAP_SYSNR", ""),
            "client": os.getenv("SAP_CLIENT", ""),
            "lang": os.getenv("SAP_LANG", "EN"),
        }
        missing = [k for k, v in conn_params.items() if not v and k != "lang"]
        if missing:
            return SapResult(ok=False, error=f"Missing SAP connection env vars: {missing}")

        conn = None
        try:
            conn = Connection(**conn_params)
            bapi_in = map_to_bapi_createfromdat2(order_payload)
            out = conn.call("BAPI_SALESORDER_CREATEFROMDAT2", **bapi_in)

            sales_doc = None
            if isinstance(out, dict):
                sales_doc = out.get("SALESDOCUMENT")
                if not sales_doc and isinstance(out.get("RETURN"), list) and out["RETURN"]:
                    first = out["RETURN"][0]
                    if isinstance(first, dict) and first.get("TYPE") == "S":
                        sales_doc = first.get("MESSAGE_V2") or first.get("MESSAGE_V1")

            if sales_doc:
                conn.call("BAPI_TRANSACTION_COMMIT", WAIT="X")
                return SapResult(ok=True, sales_order=str(sales_doc), raw=dict(out))

            return_msgs = out.get("RETURN") or []
            if not isinstance(return_msgs, list):
                return_msgs = [return_msgs] if return_msgs else []
            err_txt = "; ".join(
                [f"{m.get('TYPE')}:{m.get('MESSAGE')}" for m in return_msgs if isinstance(m, dict)]
            )[:1000]
            return SapResult(ok=False, error=err_txt or "BAPI returned no document", raw=dict(out))

        except Exception as e:
            return SapResult(ok=False, error=str(e))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass


def map_to_bapi_createfromdat2(order_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Minimal mapping to BAPI_SALESORDER_CREATEFROMDAT2.
    Tune fields per your config (doc type, sales org, etc.).
    """
    items: List[Dict[str, Any]] = order_payload.get("items") or []
    req_date_h = order_payload.get("req_delivery_date") or ""

    order_header_in = {
        "DOC_TYPE": order_payload.get("doc_type", "OR"),
        "SALES_ORG": order_payload.get("sales_org", "IN01"),
        "DISTR_CHAN": order_payload.get("dist_channel", "10"),
        "DIVISION": order_payload.get("division", "00"),
        "PURCH_NO_C": str(order_payload.get("po_number", ""))[:35],
        "REQ_DATE_H": req_date_h[:10] if req_date_h else "",
    }

    order_partners = [
        {"PARTN_ROLE": "AG", "PARTN_NUMB": str(order_payload.get("sold_to", ""))[:10]},
        {"PARTN_ROLE": "WE", "PARTN_NUMB": str(order_payload.get("ship_to", ""))[:10]},
    ]

    order_items_in: List[Dict[str, Any]] = []
    order_items_inx: List[Dict[str, Any]] = []
    order_schedules_in: List[Dict[str, Any]] = []
    order_schedules_inx: List[Dict[str, Any]] = []

    plant_default = order_payload.get("plant", "IN01")
    for idx, it in enumerate(items, start=10):
        itm_no = str(idx).zfill(6)
        mat = it.get("material") or ""
        pl = str(it.get("plant") or plant_default)[:4]
        qty = it.get("qty")
        if qty is None:
            qty = 0
        req_date = (it.get("req_date") or req_date_h)[:10] if (it.get("req_date") or req_date_h) else ""

        order_items_in.append({
            "ITM_NUMBER": itm_no,
            "MATERIAL": str(mat)[:18],
            "PLANT": pl,
        })
        order_items_inx.append({
            "ITM_NUMBER": itm_no,
            "UPDATEFLAG": "I",
            "MATERIAL": "X",
            "PLANT": "X",
        })
        order_schedules_in.append({
            "ITM_NUMBER": itm_no,
            "SCHED_LINE": "0001",
            "REQ_QTY": str(qty),
            "REQ_DATE": req_date,
        })
        order_schedules_inx.append({
            "ITM_NUMBER": itm_no,
            "SCHED_LINE": "0001",
            "UPDATEFLAG": "I",
            "REQ_QTY": "X",
            "REQ_DATE": "X",
        })

    return {
        "ORDER_HEADER_IN": order_header_in,
        "ORDER_PARTNERS": order_partners,
        "ORDER_ITEMS_IN": order_items_in,
        "ORDER_ITEMS_INX": order_items_inx,
        "ORDER_SCHEDULES_IN": order_schedules_in,
        "ORDER_SCHEDULES_INX": order_schedules_inx,
    }

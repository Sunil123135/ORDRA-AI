"""SAP validation adapters: stub (local JSON) or ECC (RFC) for customer, material, pricing."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _default_stub_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "stubs" / "sap"


@dataclass
class ValidationIssue:
    code: str
    severity: str  # INFO | WARN | BLOCK
    message: str
    field: Optional[str] = None
    item_number: Optional[str] = None


@dataclass
class MaterialInfo:
    material: str
    description: str
    uom: str
    material_group: str
    plant: str
    status: str


@dataclass
class CustomerInfo:
    customer_code: str
    name1: str
    sales_org: str
    dist_channel: str
    division: str
    shipping_condition: Optional[str] = None
    route: Optional[str] = None


@dataclass
class PricingResult:
    currency: str
    item_prices: Dict[str, float]  # ITM_NUMBER -> unit price
    issues: List[ValidationIssue]


class SapValidator:
    """
    Validation facade in two modes:
      - stub: reads local stub JSON files
      - ecc: uses RFC (pyrfc) to query SAP ECC
    """

    def __init__(self) -> None:
        self.mode = os.getenv("SAP_MODE", "stub").strip().lower()
        stub_dir = os.getenv("SAP_STUB_DIR", "")
        self.stub_dir = Path(stub_dir) if stub_dir else _default_stub_dir()
        if not self.stub_dir.is_absolute():
            self.stub_dir = _default_stub_dir()

    # ---------------------------
    # Public API
    # ---------------------------
    def validate_customer(self, sold_to: str, ship_to: str) -> Tuple[Optional[CustomerInfo], List[ValidationIssue]]:
        if self.mode == "stub":
            return self._stub_validate_customer(sold_to, ship_to)
        return self._ecc_validate_customer(sold_to, ship_to)

    def validate_materials(
        self,
        items: List[Dict[str, Any]],
        default_plant: Optional[str] = None,
    ) -> Tuple[Dict[str, MaterialInfo], List[ValidationIssue]]:
        if self.mode == "stub":
            return self._stub_validate_materials(items, default_plant)
        return self._ecc_validate_materials(items, default_plant)

    def validate_pricing(
        self,
        sold_to: str,
        items: List[Dict[str, Any]],
        currency: str = "INR",
    ) -> PricingResult:
        if self.mode == "stub":
            return self._stub_validate_pricing(sold_to, items, currency)
        return self._ecc_validate_pricing(sold_to, items, currency)

    # ---------------------------
    # STUB MODE
    # ---------------------------
    def _stub_load_json(self, name: str) -> Any:
        p = self.stub_dir / name
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    def _stub_validate_customer(
        self, sold_to: str, ship_to: str
    ) -> Tuple[Optional[CustomerInfo], List[ValidationIssue]]:
        issues: List[ValidationIssue] = []
        cust = self._stub_load_json(f"customer_{sold_to}.json")
        if not cust:
            issues.append(
                ValidationIssue("CUST_NOT_FOUND", "BLOCK", f"Sold-to {sold_to} not found in stub", "sold_to")
            )
            return None, issues

        sales_area = cust.get("sales_area") or {}
        ci = CustomerInfo(
            customer_code=sold_to,
            name1=cust.get("name1", ""),
            sales_org=sales_area.get("sales_org", ""),
            dist_channel=sales_area.get("dist_channel", ""),
            division=sales_area.get("division", ""),
            shipping_condition=(cust.get("shipping") or {}).get("shipping_condition"),
            route=(cust.get("shipping") or {}).get("route"),
        )

        pf = cust.get("partner_functions") or {}
        if ship_to != pf.get("ship_to"):
            issues.append(
                ValidationIssue(
                    "SHIP_TO_MISMATCH",
                    "WARN",
                    f"Ship-to {ship_to} differs from master {pf.get('ship_to')}",
                    "ship_to",
                )
            )

        return ci, issues

    def _stub_validate_materials(
        self, items: List[Dict[str, Any]], default_plant: Optional[str]
    ) -> Tuple[Dict[str, MaterialInfo], List[ValidationIssue]]:
        issues: List[ValidationIssue] = []
        mats = self._stub_load_json("materials_demo.json") or []
        idx = {(m["material"], m.get("plant") or ""): m for m in mats}
        out: Dict[str, MaterialInfo] = {}

        for it in items:
            itm_no = it.get("ITM_NUMBER")
            mat = it.get("MATERIAL") or ""
            plant = it.get("PLANT") or default_plant or "IN01"

            m = idx.get((mat, plant)) or idx.get((mat, ""))
            if not m:
                issues.append(
                    ValidationIssue(
                        "MAT_NOT_FOUND",
                        "BLOCK",
                        f"Material {mat} not found in plant {plant}",
                        "MATERIAL",
                        item_number=itm_no,
                    )
                )
                continue

            if (m.get("status") or "").upper() != "ACTIVE":
                issues.append(
                    ValidationIssue(
                        "MAT_INACTIVE", "BLOCK", f"Material {mat} is inactive", "MATERIAL", item_number=itm_no
                    )
                )

            out[itm_no] = MaterialInfo(
                material=m["material"],
                description=m.get("description", ""),
                uom=m.get("uom", ""),
                material_group=m.get("material_group", ""),
                plant=m.get("plant", plant),
                status=m.get("status", ""),
            )
        return out, issues

    def _stub_validate_pricing(
        self, sold_to: str, items: List[Dict[str, Any]], currency: str
    ) -> PricingResult:
        issues: List[ValidationIssue] = []
        pricing = self._stub_load_json("pricing_demo.json") or {}
        conds = pricing.get("conditions") or []
        price_by_mat = {c["material"]: float(c["price"]) for c in conds if c.get("condition_type") == "PR00"}

        item_prices: Dict[str, float] = {}
        for it in items:
            itm_no = it.get("ITM_NUMBER")
            mat = it.get("MATERIAL")
            p = price_by_mat.get(mat)
            if p is None:
                issues.append(
                    ValidationIssue(
                        "PRICE_MISSING",
                        "WARN",
                        f"No PR00 price found for {mat} in stub",
                        "pricing",
                        item_number=itm_no,
                    )
                )
                continue
            item_prices[itm_no] = p

        return PricingResult(currency=currency, item_prices=item_prices, issues=issues)

    # ---------------------------
    # ECC MODE (RFC)
    # ---------------------------
    def _ecc_conn(self) -> Any:
        from pyrfc import Connection  # type: ignore

        params = {
            "user": os.getenv("SAP_USER", ""),
            "passwd": os.getenv("SAP_PASS", ""),
            "ashost": os.getenv("SAP_ASHOST", ""),
            "sysnr": os.getenv("SAP_SYSNR", ""),
            "client": os.getenv("SAP_CLIENT", ""),
            "lang": os.getenv("SAP_LANG", "EN"),
        }
        missing = [k for k, v in params.items() if not v and k != "lang"]
        if missing:
            raise RuntimeError(f"Missing SAP connection env vars: {missing}")
        return Connection(**params)

    def _ecc_validate_customer(
        self, sold_to: str, ship_to: str
    ) -> Tuple[Optional[CustomerInfo], List[ValidationIssue]]:
        issues: List[ValidationIssue] = []
        conn = None
        try:
            conn = self._ecc_conn()
            out = conn.call("BAPI_CUSTOMER_GETDETAIL2", CUSTOMERNO=sold_to)
            name = ((out.get("CUSTOMERADDRESS") or {}).get("NAME")) or sold_to

            sales_org = os.getenv("SAP_SALES_ORG", "IN01")
            dist = os.getenv("SAP_DIST_CHANNEL", "10")
            div = os.getenv("SAP_DIVISION", "00")

            ci = CustomerInfo(
                customer_code=sold_to,
                name1=name,
                sales_org=sales_org,
                dist_channel=dist,
                division=div,
            )

            if ship_to != sold_to:
                try:
                    conn.call("BAPI_CUSTOMER_GETDETAIL2", CUSTOMERNO=ship_to)
                except Exception:
                    issues.append(
                        ValidationIssue(
                            "SHIP_TO_NOT_FOUND", "BLOCK", f"Ship-to {ship_to} not found in ECC", "ship_to"
                        )
                    )

            return ci, issues
        except Exception as e:
            issues.append(
                ValidationIssue("CUST_LOOKUP_FAIL", "BLOCK", f"Customer lookup failed: {e}", "sold_to")
            )
            return None, issues
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _ecc_validate_materials(
        self, items: List[Dict[str, Any]], default_plant: Optional[str]
    ) -> Tuple[Dict[str, MaterialInfo], List[ValidationIssue]]:
        issues: List[ValidationIssue] = []
        conn = None
        out: Dict[str, MaterialInfo] = {}
        try:
            conn = self._ecc_conn()
            default_pl = default_plant or os.getenv("SAP_DEFAULT_PLANT", "IN01")
            for it in items:
                itm_no = it.get("ITM_NUMBER")
                mat = it.get("MATERIAL") or ""
                plant = it.get("PLANT") or default_pl

                try:
                    m = conn.call("BAPI_MATERIAL_GET_DETAIL", MATERIAL=mat)
                    descs = m.get("MATERIALDESCRIPTION") or []
                    desc = (descs[0].get("MATL_DESC", "") or "") if descs else ""
                    base_uom = ((m.get("MATERIALGENERALDATA") or {}).get("BASE_UOM")) or ""

                    out[itm_no] = MaterialInfo(
                        material=mat,
                        description=desc,
                        uom=base_uom,
                        material_group=((m.get("MATERIALGENERALDATA") or {}).get("MATL_GROUP")) or "",
                        plant=plant,
                        status="ACTIVE",
                    )
                except Exception as e:
                    issues.append(
                        ValidationIssue(
                            "MAT_LOOKUP_FAIL",
                            "BLOCK",
                            f"Material lookup failed for {mat}: {e}",
                            "MATERIAL",
                            item_number=itm_no,
                        )
                    )

            return out, issues
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _ecc_validate_pricing(
        self, sold_to: str, items: List[Dict[str, Any]], currency: str
    ) -> PricingResult:
        issues: List[ValidationIssue] = [
            ValidationIssue(
                "PRICING_ECC_NOT_IMPLEMENTED",
                "INFO",
                "ECC pricing check not configured; rely on SAP pricing at creation",
                "pricing",
            )
        ]
        return PricingResult(currency=currency, item_prices={}, issues=issues)

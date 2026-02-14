from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class EvidenceSpan(BaseModel):
    source: Literal["email_body", "pdf_text", "ocr_text", "excel_sheet"]
    file_id: Optional[str] = None
    page: Optional[int] = None
    bbox: Optional[List[float]] = None
    text: str


class FieldValue(BaseModel):
    value: Optional[str] = None
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: List[EvidenceSpan] = Field(default_factory=list)
    uncertainty_reason: Optional[str] = None


class LineItem(BaseModel):
    line_no: int
    customer_material: FieldValue
    description: FieldValue
    quantity: FieldValue
    uom: FieldValue
    unit_price: FieldValue
    currency: FieldValue
    requested_delivery_date: Optional[FieldValue] = None


class ExtractedOrder(BaseModel):
    po_number: FieldValue
    po_date: FieldValue
    sold_to: FieldValue
    ship_to: FieldValue
    payer: Optional[FieldValue] = None
    incoterms: Optional[FieldValue] = None
    payment_terms: Optional[FieldValue] = None
    requested_delivery_date: Optional[FieldValue] = None
    shipping_condition: Optional[FieldValue] = None
    header_notes: Optional[FieldValue] = None
    line_items: List[LineItem]


class ValidationIssue(BaseModel):
    code: str
    severity: Literal["info", "warn", "critical"]
    message: str
    recommended_action: str
    related_fields: List[str] = Field(default_factory=list)


class SAPResolvedIds(BaseModel):
    sold_to_id: Optional[str] = None
    ship_to_id: Optional[str] = None
    payer_id: Optional[str] = None


class ValidatedOrderDraft(BaseModel):
    extracted: ExtractedOrder
    resolved_ids: SAPResolvedIds
    mapped_materials: Dict[int, Optional[str]] = Field(default_factory=dict)
    uom_ok: Dict[int, bool] = Field(default_factory=dict)
    pricing_ok: Dict[int, bool] = Field(default_factory=dict)
    credit_ok: Optional[bool] = None
    atp_ok: Optional[bool] = None
    derived_plant: Optional[str] = None
    derived_route: Optional[str] = None
    issues: List[ValidationIssue] = Field(default_factory=list)


class Decision(BaseModel):
    action: Literal["AUTO_POST", "CS_REVIEW", "ASK_CUSTOMER", "HOLD"]
    safe_to_post: bool
    confidence_overall: float = Field(ge=0.0, le=1.0)
    reasons: List[str]
    required_human_role: Optional[Literal["CS", "PRICING", "FINANCE", "COMPLIANCE"]] = None


class HITLTask(BaseModel):
    task_id: str
    job_id: str
    created_at: datetime
    status: Literal["OPEN", "APPROVED", "REJECTED", "CHANGES_REQUESTED"]
    role: Literal["CS", "PRICING", "FINANCE", "COMPLIANCE"]
    payload: Dict[str, Any]
    decision: Optional[Dict[str, Any]] = None


class AuditRecord(BaseModel):
    job_id: str
    created_at: datetime
    model_versions: Dict[str, str] = Field(default_factory=dict)
    inputs_hashes: Dict[str, str] = Field(default_factory=dict)
    extracted: Optional[ExtractedOrder] = None
    validated: Optional[ValidatedOrderDraft] = None
    decision: Optional[Decision] = None
    human_overrides: List[Dict[str, Any]] = Field(default_factory=list)
    sap_result: Optional[Dict[str, Any]] = None

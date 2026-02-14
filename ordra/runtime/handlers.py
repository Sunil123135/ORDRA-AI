from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict

from ordra.agents.decision_verifier import DecisionVerifier
from ordra.agents.verifier_agent import GeminiVerifier
from ordra.connectors.o365_client import O365Client, O365Error
from ordra.llm.openai_client import OpenAIClient, LLMError
from ordra.memory.episodic import EpisodicMemoryStore
from ordra.identity.customer_resolver import CustomerIdentityResolver
from ordra.orchestration.dag_spec import NodeSpec
from ordra.sap.sap_client import SapClient, map_to_bapi_createfromdat2
from ordra.orchestration.executor import TransientError

_ORDRA_ROOT = Path(__file__).resolve().parent.parent


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _ensure_job_id(ctx: Dict[str, Any]) -> str:
    job_id = ctx.get("job_id")
    if not job_id:
        job_id = f"J-{uuid.uuid4().hex[:10]}"
        ctx["job_id"] = job_id
    return job_id


def email_fetch_and_normalize(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    _ensure_job_id(ctx)
    # Prefer mailbox-fetched email when present (from mailbox_fetch)
    from_mailbox = ctx.get("email_message") if isinstance(ctx.get("email_message"), dict) else None
    if from_mailbox and from_mailbox.get("provider") == "o365":
        email_message = {
            "subject": from_mailbox.get("subject"),
            "from": from_mailbox.get("from"),
            "body": from_mailbox.get("body", ""),
            "received_at": from_mailbox.get("received_at") or _now_iso(),
        }
        pdf_files = ctx.get("pdf_files") or []
        excel_files = ctx.get("excel_files") or []
        has_pdf = bool(pdf_files)
        has_excel = bool(excel_files)
        attachments = [{"name": f.get("name"), "content_type": f.get("content_type")} for f in (pdf_files + excel_files)]
    else:
        email_message = {
            "subject": ctx.get("email_subject"),
            "from": ctx.get("email_from"),
            "body": ctx.get("email_body", ""),
            "received_at": ctx.get("received_at") or _now_iso(),
        }
        attachments = ctx.get("attachments") or []
        has_pdf = bool(ctx.get("pdf_text"))
        has_excel = bool(ctx.get("excel_tables"))
    return {
        "email_message": email_message,
        "attachments": attachments,
        "has_pdf_attachment": has_pdf,
        "has_excel_attachment": has_excel,
        "pdf_is_scanned_or_low_text": bool(ctx.get("ocr_text")),
    }


def agents_intake_agent(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    # Prefer identity resolution from config (resolve_customer_identity) when present
    existing = ctx.get("intake_context") or {}
    if existing.get("resolution") in ("EXACT_EMAIL_MATCH", "DOMAIN_MATCH"):
        return {"intake_context": existing}

    email = ctx.get("email_message") or {}
    sender = (email.get("from") or "").lower()
    domain = sender.split("@")[-1] if "@" in sender else None
    customer_candidate = None
    confidence = 0.2
    if domain:
        customer_candidate = domain.replace(".", "_").upper()
        confidence = 0.55
    intake_context = {
        "customer_candidate": customer_candidate,
        "customer_confidence": confidence,
        "priority": 0.5,
    }
    return {"intake_context": intake_context}


def agents_validation_agent(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    validated_order = {
        "extracted": ctx.get("extracted_order"),
        "resolved_ids": ctx.get("customer_validation", {}).get("resolved_ids", {}),
        "mapped_materials": ctx.get("material_validation", {}).get("mapped_materials", {}),
        "uom_ok": ctx.get("material_validation", {}).get("uom_ok", {}),
        "pricing_ok": ctx.get("pricing_validation", {}).get("pricing_ok", {}),
        "credit_ok": ctx.get("credit_validation", {}).get("credit_ok"),
        "atp_ok": ctx.get("atp_validation", {}).get("atp_ok"),
        "derived_plant": ctx.get("atp_validation", {}).get("derived_plant"),
        "derived_route": ctx.get("atp_validation", {}).get("derived_route"),
        "issues": (
            (ctx.get("customer_validation") or {}).get("issues", [])
            + (ctx.get("material_validation") or {}).get("issues", [])
            + (ctx.get("pricing_validation") or {}).get("issues", [])
            + (ctx.get("credit_validation") or {}).get("issues", [])
            + (ctx.get("atp_validation") or {}).get("issues", [])
        ),
    }
    return {"validated_order": validated_order}


def _make_hitl_payload(ctx: Dict[str, Any], decision: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "job_id": ctx.get("job_id"),
        "decision": decision,
        "validated_order": ctx.get("validated_order"),
        "extracted_order": ctx.get("extracted_order"),
    }


def agents_decision_agent(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    validated = ctx.get("validated_order") or {}
    policy_violations = ctx.get("policy_violations") or []
    intake = ctx.get("intake_context") or {}
    # Guardrail: never AUTO_POST for unknown senders
    if intake.get("customer_candidate") == "UNKNOWN":
        decision = {
            "action": "CS_REVIEW",
            "safe_to_post": False,
            "confidence_overall": 0.5,
            "reasons": ["Unknown sender – customer not in identity config"],
            "required_human_role": "CS",
        }
        return {"decision": decision, "hitl_task": _make_hitl_payload(ctx, decision)}

    trust_tier = intake.get("trust_tier_override") or ctx.get("trust_tier") or "BRONZE"

    if any(v.get("severity") == "hard_block" for v in policy_violations):
        reasons = [v.get("reason", "Policy violation") for v in policy_violations if v.get("severity") == "hard_block"]
        decision = {
            "action": "CS_REVIEW",
            "safe_to_post": False,
            "confidence_overall": 0.70,
            "reasons": reasons,
            "required_human_role": "CS",
        }
        return {"decision": decision, "hitl_task": _make_hitl_payload(ctx, decision)}

    extracted = validated.get("extracted") or {}
    po_number = (extracted.get("po_number") or {}).get("value")
    line_items = extracted.get("line_items") or []
    missing_core = False
    if not po_number or str(po_number).upper().endswith("UNKNOWN"):
        missing_core = True
    for li in line_items:
        cm = (li.get("customer_material") or {}).get("value")
        qty = (li.get("quantity") or {}).get("value")
        uom = (li.get("uom") or {}).get("value")
        if not cm or not qty or not uom:
            missing_core = True
            break
    mapped_materials = validated.get("mapped_materials") or {}
    has_unmapped = any(v in (None, "", "UNMAPPED") for v in mapped_materials.values()) if mapped_materials else True
    credit_ok = validated.get("credit_ok")
    if credit_ok is not True:
        decision = {
            "action": "HOLD" if credit_ok is False else "CS_REVIEW",
            "safe_to_post": False,
            "confidence_overall": 0.70,
            "reasons": ["Credit blocked" if credit_ok is False else "Credit not validated"],
            "required_human_role": "FINANCE" if credit_ok is False else "CS",
        }
        return {"decision": decision, "hitl_task": _make_hitl_payload(ctx, decision)}
    if missing_core:
        decision = {
            "action": "ASK_CUSTOMER",
            "safe_to_post": False,
            "confidence_overall": 0.60,
            "reasons": ["Missing mandatory fields"],
            "required_human_role": "CS",
        }
        return {"decision": decision, "hitl_task": _make_hitl_payload(ctx, decision)}
    if has_unmapped:
        decision = {
            "action": "CS_REVIEW",
            "safe_to_post": False,
            "confidence_overall": 0.75,
            "reasons": ["Unmapped material(s)"],
            "required_human_role": "CS",
        }
        return {"decision": decision, "hitl_task": _make_hitl_payload(ctx, decision)}

    if trust_tier == "BRONZE":
        decision = {
            "action": "CS_REVIEW",
            "safe_to_post": False,
            "confidence_overall": 0.75,
            "reasons": [f"Customer trust tier {trust_tier} – AUTO_POST not enabled"],
            "required_human_role": "CS",
        }
        return {"decision": decision, "hitl_task": _make_hitl_payload(ctx, decision)}

    decision = {
        "action": "AUTO_POST",
        "safe_to_post": True,
        "confidence_overall": 0.92,
        "reasons": ["All validations passed; safe-to-post satisfied"],
        "required_human_role": None,
    }
    return {"decision": decision, "hitl_task": None}


def agents_audit_agent(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    job_id = _ensure_job_id(ctx)
    decision = ctx.get("decision") or {}
    confidence = float(decision.get("confidence_overall", 0.0))
    intake = ctx.get("intake_context") or {}
    customer_key = intake.get("customer_candidate") or "UNKNOWN"
    trust_tier = ctx.get("trust_tier") or "BRONZE"

    pricing_path = str(_ORDRA_ROOT / "pricing" / "confidence_pricing.yaml")
    pricing_info = {"price": 0.0, "tier": "unknown"}
    if Path(pricing_path).is_file():
        from ordra.pricing.calculator import ConfidencePricing
        pricing = ConfidencePricing(pricing_path)
        pricing_info = pricing.price(confidence)

    now = _now_iso()
    audit_record = {
        "job_id": job_id,
        "created_at": now,
        "inputs": {
            "email_from": (ctx.get("email_message") or {}).get("from"),
            "email_subject": (ctx.get("email_message") or {}).get("subject"),
        },
        "extracted": ctx.get("extracted_order"),
        "validated": ctx.get("validated_order"),
        "decision": decision,
        "sap_result": ctx.get("sap_order_result"),
        "hitl_task_id": ctx.get("hitl_task_id"),
        "dag_exec": ctx.get("_dag_exec"),
        "pricing": pricing_info,
        "revenue_event": {
            "job_id": job_id,
            "customer": customer_key,
            "confidence": confidence,
            "tier": trust_tier,
            "price": pricing_info.get("price", 0.0),
            "timestamp": now,
        },
    }
    db = (ctx.get("_runtime") or {}).get("db")
    if db and hasattr(db, "connect"):
        try:
            with db.connect() as conn:
                conn.execute(
                    """
                    INSERT INTO revenue_events(job_id, customer_key, confidence, tier, price, created_at)
                    VALUES(?,?,?,?,?,?)
                    """,
                    (job_id, customer_key, confidence, trust_tier, pricing_info.get("price", 0.0), now),
                )
        except Exception:
            pass
    decision_deck = {
        "job_id": job_id,
        "headline": decision.get("action"),
        "issues": (ctx.get("validated_order") or {}).get("issues", []),
        "savings_estimate": {"minutes_saved": 20, "assumptions": "placeholder MVP"},
    }
    return {"audit_record": audit_record, "decision_deck": decision_deck}


def _pdf_bytes_to_text(data: bytes) -> str:
    try:
        from io import BytesIO
        from pypdf import PdfReader
        r = PdfReader(BytesIO(data))
        return "\n".join((page.extract_text() or "") for page in r.pages)
    except Exception:
        return ""


def documents_pdf_text_extract(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    pdf_text = ctx.get("pdf_text") or ""
    if not pdf_text and ctx.get("pdf_files"):
        first = ctx["pdf_files"][0]
        raw = first.get("bytes") if isinstance(first, dict) else getattr(first, "bytes_data", None)
        if raw:
            pdf_text = _pdf_bytes_to_text(raw)
    chunks = [pdf_text] if pdf_text else []
    return {"pdf_text_chunks": chunks}


def documents_ocr_run(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    ocr_text = ctx.get("ocr_text") or ""
    chunks = [ocr_text] if ocr_text else []
    return {"ocr_text_chunks": chunks}


def documents_excel_parse(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    excel_tables = ctx.get("excel_tables") or ""
    return {"excel_tables": excel_tables}


def memory_faiss_retrieve_layout_hints(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    return {"layout_hints": ""}


def skills_retrieve_for_context(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Load skill Markdown (order policy, SAP rules, OCR playbook, customer layouts) for extractor prompt."""
    from ordra.skills.registry import load_skills_for_context
    intake = ctx.get("intake_context") or {}
    doc_quality = ctx.get("doc_quality") or ""
    doc_type = "digital" if doc_quality == "digital" else "pdf_scan"
    skills_text = load_skills_for_context(
        customer_id=intake.get("customer_candidate"),
        doc_type=doc_type,
    )
    return {"skills_text": skills_text}


def _get_episode_store(ctx: Dict[str, Any]):
    return (ctx.get("_runtime") or {}).get("episode_store")


def memory_episode_retrieve(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """
    Retrieve prior episode skeletons (recipes) for this customer/layout.
    Injected into extraction prompt as guidance (stateful skeleton learning).
    """
    store = _get_episode_store(ctx)
    intake = ctx.get("intake_context") or {}
    customer = intake.get("customer_candidate") or "UNKNOWN"
    layout_hash = ctx.get("layout_hints") if ctx.get("layout_hints") else None
    if not store or not isinstance(store, EpisodicMemoryStore):
        return {"episode_recipes": {"recipes": [], "recipes_text": ""}}
    result = store.retrieve_recipes(customer_key=customer, layout_hash=layout_hash, limit=5)
    return {"episode_recipes": result}


def memory_episode_retrieve_recipes(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Legacy: same as memory_episode_retrieve but returns recipe_hints for backward compat."""
    out = memory_episode_retrieve(ctx, spec)
    rec = out.get("episode_recipes") or {}
    return {
        "episode_recipes": rec.get("recipes", []),
        "recipe_hints": rec.get("recipes_text", ""),
    }


def memory_episode_save(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Persist episode skeleton after finalize_audit (EpisodicMemoryStore)."""
    store = _get_episode_store(ctx)
    if not store or not isinstance(store, EpisodicMemoryStore):
        return {"episode_saved": {}}
    ep = store.save_episode(ctx)
    return {
        "episode_saved": {
            "episode_id": ep.episode_id,
            "customer_key": ep.customer_key,
            "layout_hash": ep.layout_hash,
        }
    }


def policies_evaluate(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Policy-as-code: evaluate order against YAML policies; output violations."""
    policy_path = str(_ORDRA_ROOT / "policies" / "order_policies.yaml")
    if not Path(policy_path).is_file():
        return {"policy_violations": []}
    from ordra.policies.evaluator import PolicyEvaluator
    validated = ctx.get("validated_order") or {}
    credit_ok = validated.get("credit_ok")
    facts = {
        "credit_status": "BLOCKED" if credit_ok is False else "OK",
        "credit_exposure": 0,
        "credit_limit": 1,
        "export_control_flag": False,
        "sanctioned_country": False,
        "material_trust_score": 0.9,
    }
    evaluator = PolicyEvaluator(policy_path)
    violations = evaluator.evaluate(facts)
    return {"policy_violations": violations}


def trust_evaluate(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Customer trust tier from episodic memory (AUTO_POST eligibility)."""
    store = _get_episode_store(ctx)
    intake = ctx.get("intake_context") or {}
    customer_key = intake.get("customer_candidate") or "UNKNOWN"
    if not store or not isinstance(store, EpisodicMemoryStore):
        return {"trust_tier": "BRONZE", "trust_clean_count": 0}
    try:
        trust_path = str(_ORDRA_ROOT / "trust" / "customer_trust.yaml")
        decay_path = str(_ORDRA_ROOT / "trust" / "trust_decay.yaml")
        from ordra.trust.evaluator import CustomerTrustEvaluator
        evaluator = CustomerTrustEvaluator(trust_path, store, decay_path=decay_path)
        tier, clean_count = evaluator.evaluate(customer_key)
        return {"trust_tier": tier, "trust_clean_count": clean_count}
    except Exception:
        return {"trust_tier": "BRONZE", "trust_clean_count": 0}


def memory_faiss_resolve_aliases(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    return {"alias_resolution": {}}


def llm_openai_extract_po_schema(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    client: OpenAIClient = ctx["_runtime"]["openai_client"]
    email_body = (ctx.get("email_message") or {}).get("body") or ctx.get("email_body") or ""
    intake_context = ctx.get("intake_context") or {}
    customer_hint = intake_context.get("customer_candidate")
    pdf_chunks = ctx.get("pdf_text_chunks") or []
    pdf_text = "\n".join(pdf_chunks) if pdf_chunks else (ctx.get("pdf_text") or "")
    ocr_chunks = ctx.get("ocr_text_chunks") or []
    ocr_text = "\n".join(ocr_chunks) if ocr_chunks else (ctx.get("ocr_text") or "")
    excel_tables = ctx.get("excel_tables") or ""
    inputs = {
        "customer_hint": customer_hint,
        "layout_hints": ctx.get("layout_hints") or "",
        "recipe_hints": ctx.get("recipe_hints") or "",
        "episode_recipes": ctx.get("episode_recipes"),
        "skills_text": ctx.get("skills_text") or "",
        "doc_quality": ctx.get("doc_quality"),
        "issue_codes": [],
        "email_body": email_body,
        "pdf_text": pdf_text,
        "ocr_text": ocr_text,
        "excel_tables": excel_tables,
    }
    try:
        extracted = client.extract_po_schema(inputs)
        return {"extracted_order": extracted}
    except LLMError as e:
        raise TransientError(str(e)) from e


def agents_verifier_extraction(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """
    Scores extraction quality with Gemini Flash-class verifier.
    Veto-only: can request refine or force HITL later.
    """
    verifier: GeminiVerifier = ctx["_runtime"]["verifier"]
    extracted = ctx.get("extracted_order") or {}
    customer = (ctx.get("intake_context") or {}).get("customer_candidate") or "UNKNOWN"
    query = "Extract purchase order fields into the required JSON schema."
    draft = str(extracted)[:4000]
    context = f"customer={customer}\nlayout_hints={ctx.get('layout_hints') or ''}"
    score, critique = verifier.verify(query=query, draft=draft, context=context)
    needs_refine = score < 85
    hard_fail = score < 60
    return {
        "extraction_verdict": {
            "score": score,
            "critique": critique,
            "needs_refine": bool(needs_refine),
            "hard_fail": bool(hard_fail),
        }
    }


def llm_openai_refine_po_schema(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Refine extracted_order using verifier critique (one pass)."""
    verdict = ctx.get("extraction_verdict") or {}
    if not verdict.get("needs_refine"):
        return {}
    client: OpenAIClient = ctx["_runtime"]["openai_client"]
    critique = verdict.get("critique") or ""
    extracted = ctx.get("extracted_order") or {}
    email_body = (ctx.get("email_message") or {}).get("body") or ctx.get("email_body") or ""
    pdf_text = "\n".join(ctx.get("pdf_text_chunks") or [])
    ocr_text = "\n".join(ctx.get("ocr_text_chunks") or [])
    excel_tables = ctx.get("excel_tables") or ""
    inputs = {
        "critique": critique,
        "original_extracted_order": extracted,
        "original_extracted_order_json": json.dumps(extracted, indent=2),
        "email_body": email_body,
        "pdf_text": pdf_text,
        "ocr_text": ocr_text,
        "excel_tables": excel_tables,
    }
    try:
        refined = client.refine_po_schema(inputs)
        return {"extracted_order": refined}
    except LLMError as e:
        raise TransientError(str(e)) from e


def tools_extraction_result(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Merge extraction: prefer refined (from refine_extraction node) when present, else original."""
    refined = ctx.get("refine_extraction.extracted_order") or ctx.get("refined_order")
    if refined is not None:
        return {"extracted_order": refined}
    return {"extracted_order": ctx.get("extracted_order")}


def agents_verifier_decision(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """
    Veto-only safety agent. Cannot approve AUTO_POST; can only downgrade to CS_REVIEW.
    """
    verifier = DecisionVerifier()
    decision = dict(ctx.get("decision") or {})
    validated = ctx.get("validated_order") or {}

    allow, reason = verifier.verify(
        decision=decision,
        validated_order=validated,
        context=ctx,
    )

    if not allow:
        decision["action"] = "CS_REVIEW"
        decision["safe_to_post"] = False
        decision.setdefault("reasons", []).append(reason)
        decision["verifier_veto"] = True
        decision["required_human_role"] = "CS"
        hitl_task = _make_hitl_payload(ctx, decision)
        return {
            "decision": decision,
            "hitl_task": hitl_task,
            "decision_verdict": {"allow": False, "reason": reason},
        }

    return {
        "decision": decision,
        "decision_verdict": {"allow": True, "reason": reason},
    }


def verifier_verify_decision(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Legacy veto-only verifier (OpenAI). Kept for compatibility."""
    decision = ctx.get("decision") or {}
    if decision.get("action") != "AUTO_POST":
        return {}
    client: OpenAIClient = ctx["_runtime"]["openai_client"]
    validated = ctx.get("validated_order") or {}
    summary_parts = [
        f"issues_count={len(validated.get('issues') or [])}",
        f"credit_ok={validated.get('credit_ok')}",
        f"confidence_overall={decision.get('confidence_overall')}",
    ]
    validation_summary = "; ".join(summary_parts)
    try:
        result = client.verify_decision(decision, validation_summary)
    except LLMError:
        result = {"veto": False, "reason": ""}
    if not result.get("veto"):
        return {}
    downgraded = {
        "action": "CS_REVIEW",
        "safe_to_post": False,
        "confidence_overall": decision.get("confidence_overall", 0.8),
        "reasons": (decision.get("reasons") or []) + [f"Verifier veto: {result.get('reason', 'unspecified')}"],
        "required_human_role": "CS",
    }
    hitl_task = _make_hitl_payload(ctx, downgraded)
    return {"decision": downgraded, "hitl_task": hitl_task}


def sap_validate_customer(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    ex = ctx.get("extracted_order") or {}
    sold_to_val = (ex.get("sold_to") or {}).get("value")
    ship_to_val = (ex.get("ship_to") or {}).get("value")
    issues = []
    resolved = {"sold_to_id": None, "ship_to_id": None, "payer_id": None}
    if sold_to_val:
        resolved["sold_to_id"] = "100234"
        resolved["payer_id"] = "100234"
    else:
        issues.append({"code": "CUST_MISSING", "severity": "critical", "message": "Sold-to missing", "recommended_action": "Request customer details", "related_fields": ["sold_to"]})
    if ship_to_val:
        resolved["ship_to_id"] = "200987"
    else:
        issues.append({"code": "SHIPTO_MISSING", "severity": "warn", "message": "Ship-to missing", "recommended_action": "Derive or request ship-to", "related_fields": ["ship_to"]})
    return {"customer_validation": {"resolved_ids": resolved, "issues": issues}}


def sap_validate_materials(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """
    Applies human overrides to material mapping so demo supports:
      CS override -> re-run -> AUTO_POST

    Supported overrides:
      ctx["human_overrides"]["material_mappings"] = { "1": "0000098765", "2": "0000011223" }
    """
    ex = ctx.get("extracted_order") or {}
    line_items = ex.get("line_items") or []
    overrides = ctx.get("human_overrides") or {}
    material_overrides = overrides.get("material_mappings") or {}
    norm_overrides: Dict[int, str] = {}
    if isinstance(material_overrides, dict):
        for k, v in material_overrides.items():
            try:
                ln = int(k)
                if isinstance(v, str) and v.strip():
                    norm_overrides[ln] = v.strip()
            except Exception:
                continue
    mapped_materials: Dict[int, Any] = {}
    uom_ok: Dict[int, bool] = {}
    issues = []
    for li in line_items:
        ln = int(li.get("line_no"))
        cm = (li.get("customer_material") or {}).get("value")
        if ln in norm_overrides:
            mapped_materials[ln] = norm_overrides[ln]
            uom_ok[ln] = True
            continue
        if cm:
            mapped_materials[ln] = "0000098765"
            uom_ok[ln] = True
        else:
            mapped_materials[ln] = None
            uom_ok[ln] = False
            issues.append({
                "code": "MAT_UNMAPPED",
                "severity": "critical",
                "message": f"Unmapped material at line {ln}",
                "recommended_action": "CS to map customer material",
                "related_fields": [f"line_items[{ln}].customer_material"]
            })
    return {"material_validation": {"mapped_materials": mapped_materials, "uom_ok": uom_ok, "issues": issues}}


def sap_validate_pricing(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    ex = ctx.get("extracted_order") or {}
    line_items = ex.get("line_items") or []
    pricing_ok = {}
    for li in line_items:
        ln = int(li.get("line_no"))
        pricing_ok[ln] = True
    return {"pricing_validation": {"pricing_ok": pricing_ok, "issues": []}}


def sap_validate_credit(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    forced = ctx.get("force_credit_block")
    if forced:
        return {"credit_validation": {"credit_ok": False, "issues": [{"code": "CREDIT_BLOCK", "severity": "critical", "message": "Credit blocked", "recommended_action": "Finance approval required", "related_fields": []}]}}
    return {"credit_validation": {"credit_ok": True, "issues": []}}


def sap_validate_atp(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    forced = ctx.get("force_atp_short")
    if forced:
        return {"atp_validation": {"atp_ok": False, "derived_plant": "IN01", "derived_route": "ROAD", "issues": [{"code": "ATP_SHORT", "severity": "warn", "message": "ATP short", "recommended_action": "CS review: partial/backorder", "related_fields": []}]}}
    return {"atp_validation": {"atp_ok": True, "derived_plant": "IN01", "derived_route": "ROAD", "issues": []}}


def _build_sap_order_payload(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Build SAP order payload from validated/extracted order and intake identity."""
    intake = ctx.get("intake_context") or {}
    validated = ctx.get("validated_order") or {}
    extracted = validated.get("extracted") or ctx.get("extracted_order") or {}
    cust = ctx.get("customer_validation") or {}
    resolved = cust.get("resolved_ids") or {}
    materials = (ctx.get("material_validation") or {}).get("mapped_materials") or {}
    atp = ctx.get("atp_validation") or {}

    sold_to = intake.get("sold_to") or resolved.get("sold_to_id") or (extracted.get("sold_to") or {}).get("value")
    ship_to = intake.get("ship_to") or resolved.get("ship_to_id") or (extracted.get("ship_to") or {}).get("value")
    po_number = (extracted.get("po_number") or {}).get("value")
    req_delivery_date = (extracted.get("requested_delivery_date") or {}).get("value") or ""

    sold_to = str(sold_to or "")[:10]
    ship_to = str(ship_to or "")[:10]
    plant_default = atp.get("derived_plant") or "IN01"

    items = []
    for li in extracted.get("line_items") or []:
        ln = int(li.get("line_no", 0))
        mat = materials.get(ln)
        if mat is None:
            mat = (li.get("customer_material") or {}).get("value")
        qty_val = (li.get("quantity") or {}).get("value")
        try:
            qty = float(qty_val) if qty_val is not None else 0
        except (TypeError, ValueError):
            qty = 0
        line_req = (li.get("requested_delivery_date") or {}).get("value") or req_delivery_date
        items.append({
            "material": str(mat or "")[:18],
            "plant": plant_default,
            "qty": qty,
            "req_date": str(line_req)[:10] if line_req else req_delivery_date[:10] if req_delivery_date else "",
        })

    return {
        "sold_to": sold_to or "1040402",
        "ship_to": ship_to or "1040402",
        "po_number": str(po_number or "")[:35],
        "req_delivery_date": str(req_delivery_date)[:10] if req_delivery_date else "",
        "doc_type": "OR",
        "sales_org": "IN01",
        "dist_channel": "10",
        "division": "00",
        "plant": plant_default,
        "items": items,
    }


def _make_extracted_bapi(bapi_payload: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
    email_msg = ctx.get("email_message") or {}
    return {
        **bapi_payload,
        "meta": {
            "source_email": email_msg.get("from"),
            "internet_message_id": ctx.get("internet_message_id"),
            "extraction_confidence": (ctx.get("validated_order") or {}).get("confidence_overall"),
        },
    }


def sap_build_bapi_preview(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Build BAPI-aligned payload for preview (runs for all jobs that reach validation)."""
    payload = _build_sap_order_payload(ctx)
    bapi_payload = map_to_bapi_createfromdat2(payload)
    extracted_bapi = _make_extracted_bapi(bapi_payload, ctx)
    return {"extracted_bapi": extracted_bapi, "sap_bapi_payload": bapi_payload}


def sap_create_sales_order(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    client = (ctx.get("_runtime") or {}).get("sap_client")
    if not client or not isinstance(client, SapClient):
        return {"sap_order_result": {"sap_order_number": None, "sap_post_error": "sap_client not available"}}

    payload = ctx.get("sap_order_payload")
    if not payload:
        payload = _build_sap_order_payload(ctx)

    bapi_payload = map_to_bapi_createfromdat2(payload)
    email_msg = ctx.get("email_message") or {}
    extracted_bapi = _make_extracted_bapi(bapi_payload, ctx)

    res = client.create_sales_order(payload)
    if res.ok:
        return {
            "sap_order_result": {
                "sap_order_number": res.sales_order,
                "sap_post_raw": res.raw,
            },
            "extracted_bapi": extracted_bapi,
            "sap_bapi_payload": bapi_payload,
        }
    return {
        "sap_order_result": {
            "sap_order_number": None,
            "sap_post_error": res.error,
            "sap_post_raw": res.raw,
        },
        "extracted_bapi": extracted_bapi,
        "sap_bapi_payload": bapi_payload,
    }


def hitl_create_task_if_needed(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    decision = ctx.get("decision") or {}
    job_id = _ensure_job_id(ctx)
    task_id = f"T-{uuid.uuid4().hex[:10]}"
    payload = ctx.get("hitl_task") or {
        "job_id": job_id,
        "decision": decision,
        "validated_order": ctx.get("validated_order"),
        "extracted_order": ctx.get("extracted_order"),
        "override_template": {
            "material_mappings": {}
        }
    }
    tasks = ctx["_runtime"].setdefault("hitl_tasks", {})
    tasks[task_id] = {
        "task_id": task_id,
        "job_id": job_id,
        "created_at": _now_iso(),
        "status": "OPEN",
        "role": decision.get("required_human_role") or "CS",
        "payload": payload,
    }
    return {"hitl_task_id": task_id}


def documents_doc_quality_router(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """
    Document quality classifier: digital vs scanned_good vs scanned_poor.
    Sets doc_quality and preprocess_plan for downstream OCR branching.
    MVP: heuristic from text length and whether OCR path was requested.
    """
    pdf_text = ctx.get("pdf_text") or ""
    has_ocr = bool(ctx.get("ocr_text"))
    pdf_len = len(pdf_text.strip())
    if has_ocr or (pdf_len < 50 and ctx.get("has_pdf_attachment")):
        doc_quality = "scanned_poor" if pdf_len < 20 else "scanned_good"
        preprocess_plan = ["ocr_fast"] if pdf_len >= 20 else ["ocr_strong", "table_extractor"]
    else:
        doc_quality = "digital"
        preprocess_plan = ["pdf_text_extract"]
    return {
        "doc_quality": doc_quality,
        "preprocess_plan": preprocess_plan,
    }


def identity_resolve_customer(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Resolve sender email to customer_key and SAP sold-to/ship-to from config."""
    resolver = CustomerIdentityResolver()
    email = ctx.get("email_message") or {}
    sender = email.get("from")
    resolved = resolver.resolve(sender)

    if not resolved:
        return {
            "intake_context": {
                "customer_candidate": "UNKNOWN",
                "resolution": "UNMAPPED_SENDER",
            }
        }

    out = {
        "customer_candidate": resolved["customer_key"],
        "sold_to": resolved["sold_to"],
        "ship_to": resolved["ship_to"],
        "resolution": resolved.get("resolution", "EXACT_EMAIL_MATCH"),
        "customer_confidence": 0.9,
        "priority": 0.5,
    }
    if resolved.get("trust_tier_override") is not None:
        out["trust_tier_override"] = resolved["trust_tier_override"]
    if resolved.get("auto_post_enabled") is not None:
        out["auto_post_enabled"] = resolved["auto_post_enabled"]
    return {"intake_context": out}


def mailbox_o365_search(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Uses deterministic filters; returns candidates. Expects ctx['mailbox_query'] optionally."""
    client = (ctx.get("_runtime") or {}).get("o365_client")
    if not client or not isinstance(client, O365Client):
        return {"email_candidates": [], "email_message_id": None, "internet_message_id": None}

    q = ctx.get("mailbox_query") or {}
    folder = q.get("folder", "Inbox")
    from_addresses = q.get("from_addresses") or []
    subject_contains = q.get("subject_contains") or []
    has_attachments = q.get("has_attachments", True)
    received_after_iso = q.get("received_after_iso")
    max_results = int(q.get("max_results", 10))

    candidates = client.search_messages(
        folder=folder,
        from_addresses=from_addresses,
        subject_contains=subject_contains,
        has_attachments=has_attachments,
        received_after_iso=received_after_iso,
        max_results=max_results,
    )
    top = candidates[0] if candidates else None
    return {
        "email_candidates": [c.__dict__ for c in candidates],
        "email_message_id": top.id if top else None,
        "internet_message_id": top.internet_message_id if top else None,
    }


def mailbox_o365_fetch(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    client = (ctx.get("_runtime") or {}).get("o365_client")
    message_id = ctx.get("email_message_id")
    if not client or not message_id:
        return {"email_message": None}

    msg = client.fetch_message(message_id)
    sender = (((msg.get("from") or {}).get("emailAddress") or {}).get("address")) or ""
    subject = msg.get("subject") or ""
    received = msg.get("receivedDateTime") or ""
    body = (msg.get("body") or {}).get("content") or ""
    return {
        "email_message": {
            "provider": "o365",
            "message_id": msg.get("id"),
            "internet_message_id": msg.get("internetMessageId"),
            "conversation_id": msg.get("conversationId"),
            "from": sender,
            "subject": subject,
            "received_at": received,
            "body": body,
            "has_attachments": bool(msg.get("hasAttachments")),
        }
    }


def mailbox_o365_attachments(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    client = (ctx.get("_runtime") or {}).get("o365_client")
    email = ctx.get("email_message") or {}
    message_id = email.get("message_id")
    if not client or not message_id:
        return {"pdf_files": [], "excel_files": []}

    files = client.download_file_attachments(message_id)
    pdf_files = []
    excel_files = []
    for f in files:
        name_l = f.name.lower()
        entry = {"name": f.name, "content_type": f.content_type, "bytes": f.bytes_data}
        if name_l.endswith(".pdf") or f.content_type == "application/pdf":
            pdf_files.append(entry)
        elif name_l.endswith((".xlsx", ".xls", ".csv")):
            excel_files.append(entry)
    return {"pdf_files": pdf_files, "excel_files": excel_files}


def mailbox_o365_route(ctx: Dict[str, Any], spec: NodeSpec) -> Dict[str, Any]:
    """Route the email after processing: Processed / Failed / Needs CS."""
    client = (ctx.get("_runtime") or {}).get("o365_client")
    email = ctx.get("email_message") or {}
    message_id = email.get("message_id")
    if not client or not message_id:
        return {"mailbox_routed": {"skipped": True, "reason": "no client or message_id"}}

    decision = ctx.get("decision") or {}
    action = decision.get("action") or "UNKNOWN"
    sap_order_number = (ctx.get("sap_order_result") or {}).get("sap_order_number")
    dag_exec = ctx.get("_dag_exec") or {}
    had_failure = bool(dag_exec.get("failed"))

    if had_failure:
        route = "Failed"
    elif action == "AUTO_POST" and sap_order_number:
        route = "Processed"
    elif action in {"CS_REVIEW", "ASK_CUSTOMER", "HOLD"}:
        route = "Needs CS"
    else:
        route = "Needs CS"

    try:
        resp = client.route_message(message_id, route)
        return {"mailbox_routed": {"route": route, "message_id": message_id, "graph_response": resp}}
    except O365Error as e:
        return {"mailbox_routed": {"route": route, "message_id": message_id, "error": str(e)}}


def build_handlers() -> Dict[str, Callable[[Dict[str, Any], NodeSpec], Dict[str, Any]]]:
    return {
        "email.fetch_and_normalize": email_fetch_and_normalize,
        "agents.intake_agent": agents_intake_agent,
        "agents.validation_agent": agents_validation_agent,
        "agents.decision_agent": agents_decision_agent,
        "agents.audit_agent": agents_audit_agent,
        "documents.pdf_text.extract": documents_pdf_text_extract,
        "documents.ocr.run": documents_ocr_run,
        "documents.excel.parse": documents_excel_parse,
        "documents.doc_quality_router": documents_doc_quality_router,
        "skills.retrieve_for_context": skills_retrieve_for_context,
        "policies.evaluate": policies_evaluate,
        "trust.evaluate": trust_evaluate,
        "memory.faiss.retrieve_layout_hints": memory_faiss_retrieve_layout_hints,
        "memory.episode.retrieve": memory_episode_retrieve,
        "memory.episode.retrieve_recipes": memory_episode_retrieve_recipes,
        "memory.faiss.resolve_aliases": memory_faiss_resolve_aliases,
        "llm.openai.extract_po_schema": llm_openai_extract_po_schema,
        "agents.verifier_extraction": agents_verifier_extraction,
        "llm.openai.refine_po_schema": llm_openai_refine_po_schema,
        "tools.extraction_result": tools_extraction_result,
        "agents.verifier_decision": agents_verifier_decision,
        "verifier.verify_decision": verifier_verify_decision,
        "memory.episode.save": memory_episode_save,
        "sap.validate_customer": sap_validate_customer,
        "sap.validate_materials": sap_validate_materials,
        "sap.validate_pricing": sap_validate_pricing,
        "sap.validate_credit": sap_validate_credit,
        "sap.validate_atp": sap_validate_atp,
        "sap.create_sales_order": sap_create_sales_order,
        "hitl.create_task_if_needed": hitl_create_task_if_needed,
        "identity.resolve_customer": identity_resolve_customer,
        "sap.build_bapi_preview": sap_build_bapi_preview,
        "mailbox.o365.search": mailbox_o365_search,
        "mailbox.o365.fetch": mailbox_o365_fetch,
        "mailbox.o365.attachments": mailbox_o365_attachments,
        "mailbox.o365.route": mailbox_o365_route,
    }

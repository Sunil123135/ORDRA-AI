from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from jinja2 import Environment, FileSystemLoader, StrictUndefined
from jsonschema import validate as jsonschema_validate
from jsonschema import ValidationError as JSONSchemaValidationError

from ordra.skills.loader import SkillLoader


class LLMError(Exception):
    pass


@dataclass
class LLMConfig:
    model_chat: str = "gpt-4o-mini"
    temperature: float = 0.0
    max_output_tokens: int = 2000
    retries: int = 2
    backoff_seconds: Tuple[int, ...] = (2, 5)


def _load_json_schema(schema_path: str) -> Dict[str, Any]:
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _safe_parse_json(text: str) -> Dict[str, Any]:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`").strip()
    if not (text.startswith("{") or text.startswith("[")):
        raise LLMError("Model output is not JSON")
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise LLMError(f"Invalid JSON: {e}") from e


def _load_prompt(path: str, **kwargs: Any) -> str:
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    for k, v in kwargs.items():
        content = content.replace("{{ " + k + " }}", str(v))
        content = content.replace("{{" + k + "}}", str(v))
    return content


class OpenAIClient:
    def __init__(
        self,
        *,
        prompts_dir: str,
        schemas_dir: str,
        config: Optional[LLMConfig] = None,
        skills_dir: Optional[str] = None,
    ):
        self.config = config or LLMConfig()
        self.api_key = os.getenv("OPENAI_API_KEY", "").strip()
        prompts_path = Path(prompts_dir)
        self.prompts_dir = prompts_path
        self.env = Environment(
            loader=FileSystemLoader(str(prompts_path)),
            undefined=StrictUndefined,
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self.schemas_dir = Path(schemas_dir)
        self.skill_loader = SkillLoader(skills_dir=skills_dir) if skills_dir else SkillLoader()

    def extract_po_schema(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Render extract_po.j2 and validate against extracted_order.schema.json.
        Injects skill markdown (customer/layout/issue-specific) into the prompt variables.
        """
        schema_path = self.schemas_dir / "extracted_order.schema.json"
        schema = _load_json_schema(str(schema_path))

        customer_hint = inputs.get("customer_hint")
        doc_quality = inputs.get("doc_quality")
        issue_codes = inputs.get("issue_codes") or []

        selected = self.skill_loader.select_skills(
            customer_hint=customer_hint,
            issue_codes=issue_codes,
            doc_quality=doc_quality,
        )
        skills_block = SkillLoader.render_skills_block(selected)

        recipes_text = ""
        if inputs.get("episode_recipes") and isinstance(inputs["episode_recipes"], dict):
            recipes_text = inputs["episode_recipes"].get("recipes_text", "") or ""

        templ_inputs = dict(inputs)
        templ_inputs["active_skills_block"] = skills_block
        templ_inputs["episodic_recipes"] = recipes_text

        prompt = self._render("extract_po.j2", templ_inputs)
        return self._call_and_validate(
            system_prompt=None,
            user_prompt=prompt,
            schema=schema,
            stub=self._stub_extracted_order(inputs),
        )

    def refine_po_schema(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Refines extracted_order based on verifier critique.
        Expects inputs: original_extracted_order + critique + same context fields used in extraction.
        """
        schema_path = self.schemas_dir / "extracted_order.schema.json"
        schema = _load_json_schema(str(schema_path))
        prompt = self._render("refine_po.j2", inputs)
        return self._call_and_validate(
            system_prompt=None,
            user_prompt=prompt,
            schema=schema,
            stub=self._stub_extracted_order(inputs),
        )

    def _call_and_validate(
        self,
        *,
        system_prompt: Optional[str],
        user_prompt: str,
        schema: Dict[str, Any],
        stub: Dict[str, Any],
    ) -> Dict[str, Any]:
        retries = max(0, int(self.config.retries))
        backoff = list(self.config.backoff_seconds) or [2]
        last_err: Optional[str] = None
        for attempt in range(1, retries + 2):
            try:
                if not self.api_key:
                    candidate = stub
                else:
                    raw = self._call_openai_chat(system_prompt=system_prompt, user_prompt=user_prompt)
                    candidate = _safe_parse_json(raw)
                jsonschema_validate(instance=candidate, schema=schema)
                return candidate
            except (LLMError, JSONSchemaValidationError) as e:
                last_err = str(e)
                if attempt >= retries + 1:
                    raise LLMError(f"LLM failed after {attempt} attempts: {last_err}") from e
                time.sleep(backoff[min(attempt - 1, len(backoff) - 1)])
        raise LLMError(f"LLM failed: {last_err}")

    def _render(self, template_name: str, inputs: Dict[str, Any]) -> str:
        tmpl = self.env.get_template(template_name)
        return tmpl.render(**inputs)

    def _call_openai_chat(self, *, system_prompt: Optional[str], user_prompt: str) -> str:
        try:
            from openai import OpenAI
        except ImportError:
            raise LLMError(
                "OPENAI_API_KEY is set but openai package not installed. pip install openai"
            )
        client = OpenAI(api_key=self.api_key)
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})
        resp = client.chat.completions.create(
            model=self.config.model_chat,
            temperature=self.config.temperature,
            max_tokens=self.config.max_output_tokens,
            messages=messages,
        )
        content = resp.choices[0].message.content
        if not content:
            raise LLMError("Empty response from model")
        return content

    def _stub_extracted_order(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        pdf_text = (inputs.get("pdf_text") or "")[:2000]
        email_body = (inputs.get("email_body") or "")[:1000]
        po_num = "PO-UNKNOWN"
        if pdf_text and "PO" in pdf_text:
            for tok in pdf_text.replace("\n", " ").split():
                if tok.upper().startswith("PO") and any(c.isdigit() for c in tok):
                    po_num = tok.strip()
                    break
        return {
            "po_number": {"value": po_num, "confidence": 0.60, "evidence": [{"source": "pdf_text", "text": po_num}], "uncertainty_reason": None if po_num != "PO-UNKNOWN" else "PO number not confidently detected"},
            "po_date": {"value": None, "confidence": 0.10, "evidence": [], "uncertainty_reason": "PO date not found"},
            "sold_to": {"value": None, "confidence": 0.10, "evidence": [], "uncertainty_reason": "Sold-to not found"},
            "ship_to": {"value": None, "confidence": 0.10, "evidence": [], "uncertainty_reason": "Ship-to not found"},
            "payer": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Payer not found"},
            "incoterms": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Incoterms not found"},
            "payment_terms": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Payment terms not found"},
            "requested_delivery_date": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Requested delivery date not found"},
            "shipping_condition": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Shipping condition not found"},
            "header_notes": {"value": email_body[:120] if email_body else None, "confidence": 0.30, "evidence": [{"source": "email_body", "text": email_body[:120]}] if email_body else [], "uncertainty_reason": None},
            "line_items": [
                {
                    "line_no": 1,
                    "customer_material": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Customer material not found"},
                    "description": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Description not found"},
                    "quantity": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Quantity not found"},
                    "uom": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "UOM not found"},
                    "unit_price": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Unit price not found"},
                    "currency": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Currency not found"},
                    "requested_delivery_date": {"value": None, "confidence": 0.0, "evidence": [], "uncertainty_reason": "Line requested date not found"}
                }
            ]
        }

    def verify_extraction(
        self, extracted_order: Dict[str, Any], context_snippet: str = "", *, prompts_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """Verifier (small model): score extraction quality; may request one refinement. Returns passed, score, needs_refinement, critique."""
        prompts_path = Path(prompts_dir or self.prompts_dir)
        prompt = _load_prompt(
            str(prompts_path / "verifier_extraction.txt"),
            extracted_order=json.dumps(extracted_order, indent=0)[:4000],
            context_snippet=(context_snippet or "")[:500],
        )
        raw = self._call_openai_chat(system_prompt=None, user_prompt=prompt)
        out = _safe_parse_json(raw)
        return {
            "passed": bool(out.get("passed", False)),
            "score": float(out.get("score", 0.0)),
            "needs_refinement": bool(out.get("needs_refinement", False)),
            "critique": str(out.get("critique", "") or ""),
        }

    def verify_decision(
        self, decision: Dict[str, Any], validation_summary: str, *, prompts_dir: Optional[str] = None
    ) -> Dict[str, Any]:
        """Verifier (veto-only): can downgrade AUTO_POST to CS_REVIEW; cannot approve AUTO_POST. Returns veto, reason."""
        prompts_path = Path(prompts_dir or self.prompts_dir)
        prompt = _load_prompt(
            str(prompts_path / "verifier_decision.txt"),
            decision=json.dumps(decision, indent=0),
            validation_summary=validation_summary[:1000],
        )
        raw = self._call_openai_chat(system_prompt=None, user_prompt=prompt)
        out = _safe_parse_json(raw)
        return {
            "veto": bool(out.get("veto", False)),
            "reason": str(out.get("reason", "") or ""),
        }

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


class VerifierError(Exception):
    pass


@dataclass
class VerifierConfig:
    model: str = "gemini-2.5-flash-lite"  # Flash-class
    temperature: float = 0.0
    max_output_tokens: int = 600
    retries: int = 1
    backoff_seconds: Tuple[int, ...] = (1, 2)


def _parse_score_critique(text: str) -> Tuple[int, str]:
    """
    Robust parsing: SCORE:\d and CRITIQUE:...
    If missing, return conservative values.
    """
    text = (text or "").strip()
    score_match = re.search(r"SCORE:\s*(\d+)", text, re.IGNORECASE)
    critique_match = re.search(r"CRITIQUE:\s*(.*)", text, re.IGNORECASE | re.DOTALL)

    score = int(score_match.group(1)) if score_match else 0
    critique = critique_match.group(1).strip() if critique_match else text[:500]
    score = max(0, min(100, score))
    return score, critique


class GeminiVerifier:
    """
    Minimal Gemini REST caller for verification.
    Uses env var GEMINI_API_KEY. If missing, returns "verification skipped".
    """

    def __init__(self, config: Optional[VerifierConfig] = None):
        self.config = config or VerifierConfig()
        self.api_key = os.getenv("GEMINI_API_KEY", "").strip()

    def verify(self, query: str, draft: str, context: str = "") -> Tuple[int, str]:
        if not self.api_key:
            return 100, "Verification skipped (GEMINI_API_KEY missing)"

        prompt = f"""
[TASK]
You are a quality assurance verifier.
Evaluate the candidate output against the user's query and context.

[USER QUERY]
{query}

[CONTEXT]
{context}

[CANDIDATE OUTPUT]
{draft}

[INSTRUCTIONS]
1) Score 0-100 for correctness, completeness, and safety.
2) Provide concrete critique (missing fields, inconsistencies, unsafe assumptions).
3) Output strict format:
SCORE: <number>
CRITIQUE: <text>
"""

        last_err: Optional[str] = None
        for attempt in range(1, self.config.retries + 2):
            try:
                text = self._call_gemini(prompt)
                return _parse_score_critique(text)
            except Exception as e:
                last_err = str(e)
                if attempt >= self.config.retries + 1:
                    return 50, f"Verification failed: {last_err}"
                time.sleep(self.config.backoff_seconds[min(attempt - 1, len(self.config.backoff_seconds) - 1)])

        return 50, f"Verification failed: {last_err or 'unknown'}"

    def _call_gemini(self, prompt: str) -> str:
        """
        SDK-less REST call (keeps repo lightweight).
        You can swap to official SDK later.
        """
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.config.model}:generateContent?key={self.api_key}"

        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": self.config.temperature,
                "maxOutputTokens": self.config.max_output_tokens,
            },
        }

        r = requests.post(url, json=payload, timeout=30)
        if r.status_code != 200:
            raise VerifierError(f"Gemini API error {r.status_code}: {r.text[:300]}")

        data = r.json()
        candidates = data.get("candidates") or []
        if not candidates:
            raise VerifierError("No candidates returned by Gemini")

        parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
        if not parts:
            raise VerifierError("No parts returned by Gemini")

        return (parts[0].get("text") or "").strip()

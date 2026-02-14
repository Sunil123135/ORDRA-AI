from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Skill:
    name: str
    description: str
    intent_triggers: List[str]
    md_text: str


def _parse_frontmatter(md: str) -> Tuple[Dict[str, Any], str]:
    md = md or ""
    if not md.startswith("---"):
        return {}, md

    parts = md.split("---", 2)
    if len(parts) < 3:
        return {}, md

    fm_raw = parts[1].strip()
    body = parts[2].strip()

    meta: Dict[str, Any] = {}
    for line in fm_raw.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        k = k.strip()
        v = v.strip()
        # very small parser: list in [a,b] or quoted string
        if v.startswith("[") and v.endswith("]"):
            items = []
            inner = v[1:-1].strip()
            if inner:
                for tok in inner.split(","):
                    items.append(tok.strip().strip('"').strip("'"))
            meta[k] = items
        else:
            meta[k] = v.strip('"').strip("'")
    return meta, body


class SkillLoader:
    """
    Loads markdown skills and selects relevant ones per run.
    """

    def __init__(self, skills_dir: Optional[str] = None):
        self.skills_dir = Path(skills_dir) if skills_dir else Path(__file__).resolve().parent

    def load_all(self) -> List[Skill]:
        skills: List[Skill] = []
        if not self.skills_dir.exists():
            return skills

        for p in sorted(self.skills_dir.glob("*.md")):
            md = p.read_text(encoding="utf-8")
            meta, body = _parse_frontmatter(md)
            name = str(meta.get("name") or p.stem)
            desc = str(meta.get("description") or "")
            triggers = meta.get("intent_triggers") or []
            if isinstance(triggers, str):
                triggers = [triggers]
            triggers = [str(x) for x in triggers]
            skills.append(Skill(name=name, description=desc, intent_triggers=triggers, md_text=body))
        return skills

    def select_skills(
        self,
        *,
        customer_hint: Optional[str],
        issue_codes: Optional[List[str]] = None,
        doc_quality: Optional[str] = None,
    ) -> List[Skill]:
        """
        Simple matching:
          - customer-specific skills: trigger like "customer:ACME_CORP"
          - issue-specific skills: trigger like "issue:MAT_UNMAPPED"
          - doc-quality skills: trigger like "doc:scanned_poor"
        """
        issue_codes = issue_codes or []
        all_skills = self.load_all()

        wanted = []
        for s in all_skills:
            trig = [t.lower() for t in s.intent_triggers]

            if customer_hint and f"customer:{customer_hint.lower()}" in trig:
                wanted.append(s)
                continue

            if doc_quality and f"doc:{doc_quality.lower()}" in trig:
                wanted.append(s)
                continue

            for code in issue_codes:
                if f"issue:{str(code).lower()}" in trig:
                    wanted.append(s)
                    break

        # de-dup by name
        seen = set()
        out = []
        for s in wanted:
            if s.name in seen:
                continue
            seen.add(s.name)
            out.append(s)
        return out

    @staticmethod
    def render_skills_block(skills: List[Skill]) -> str:
        if not skills:
            return ""

        blocks = []
        for s in skills:
            blocks.append(
                f"### Active Skill: {s.name}\n"
                f"{s.description}\n\n"
                f"{s.md_text.strip()}\n"
            )
        return "\n\n".join(blocks).strip()

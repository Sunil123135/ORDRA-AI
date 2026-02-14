"""Generate a realistic PO PDF for ORDRA demo. Requires: pip install reportlab (or uv sync --extra demo)."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle


def build_po_pdf(
    out_path: str,
    po_number: str = "4500-ORDRA-DEMO",
    sold_to: str = "1040402",
    ship_to: str = "1040402",
) -> None:
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(
        out_path,
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=15 * mm,
        bottomMargin=15 * mm,
    )

    story = []
    story.append(Paragraph("<b>PURCHASE ORDER</b>", styles["Title"]))
    story.append(Spacer(1, 8))

    meta = [
        ["PO Number", po_number, "PO Date", str(date.today())],
        ["Sold-To", sold_to, "Ship-To", ship_to],
        ["Requested Delivery Date", "2026-02-15", "Currency", "INR"],
    ]
    t = Table(meta, colWidths=[35 * mm, 55 * mm, 40 * mm, 45 * mm])
    t.setStyle(
        TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("BACKGROUND", (0, 0), (0, -1), colors.whitesmoke),
            ("BACKGROUND", (2, 0), (2, -1), colors.whitesmoke),
        ])
    )
    story.append(t)
    story.append(Spacer(1, 10))

    story.append(Paragraph("<b>Ship-To Address</b>", styles["Heading3"]))
    story.append(
        Paragraph(
            "QuidelOrtho Demo Location<br/>Hyderabad, Telangana, India",
            styles["BodyText"],
        )
    )
    story.append(Spacer(1, 10))

    story.append(Paragraph("<b>Line Items</b>", styles["Heading3"]))

    items = [
        ["Item", "Material", "Description", "Qty", "UOM", "Unit Price", "Net Value"],
        ["10", "MAT-100001", "Reagent Pack A", "5", "EA", "1250.00", "6250.00"],
        ["20", "MAT-200010", "Control Kit B", "2", "EA", "2100.00", "4200.00"],
    ]
    it = Table(
        items,
        colWidths=[12 * mm, 25 * mm, 60 * mm, 12 * mm, 12 * mm, 22 * mm, 22 * mm],
    )
    it.setStyle(
        TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 0), (-1, -1), 8.5),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN", (3, 1), (-1, -1), "RIGHT"),
        ])
    )
    story.append(it)
    story.append(Spacer(1, 12))

    story.append(Paragraph("<b>Notes</b>", styles["Heading3"]))
    story.append(
        Paragraph(
            "Please process as per standard terms. Contact for clarification if needed.",
            styles["BodyText"],
        )
    )

    doc.build(story)


if __name__ == "__main__":
    out = Path(__file__).resolve().parent / "PO_4500-ORDRA-DEMO_1040402.pdf"
    build_po_pdf(str(out))
    print(f"Generated {out}")

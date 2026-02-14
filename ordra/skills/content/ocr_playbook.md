# OCR playbook (when to use which OCR, preprocessing)

- **Digital PDF**: Use pdf_text_extract only; no OCR.
- **Scanned good** (readable): ocr_fast; table structure usually preserved.
- **Scanned poor**: ocr_strong + table_extractor; consider de-skew, binarize. If confidence low → do not guess; ASK_CUSTOMER for better copy.

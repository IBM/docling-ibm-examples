# The compliance assessment form

[`docs/compliance_assessment_form.json`](docs/compliance_assessment_form.json) has eight
fields. No single document contains all of them, so Bob has to convert and navigate all
three.

| # | Field | Type | Found in |
|---|---|---|---|
| 1 | `entity_name` | string | `vantara_risk_disclosure.pdf` |
| 2 | `reporting_period` | string | `vantara_risk_disclosure.pdf` |
| 3 | `total_risk_exposure_usd` | number | `vantara_risk_disclosure.pdf` (repeated in the DOCX) |
| 4 | `highest_risk_category` | string | `vantara_risk_disclosure.pdf` |
| 5 | `regulator_reference_number` | string | `vantara_risk_disclosure.pdf` (repeated in the DOCX) |
| 6 | `key_obligations` | list of strings | `regulatory_guidelines_2026.epub` |
| 7 | `material_findings` | list of strings | `q3_audit_summary.docx` |
| 8 | `sign_off_date` | date | `q3_audit_summary.docx` |

## The documents

| File | Format | Contents |
|---|---|---|
| `docs/vantara_risk_disclosure.pdf` | PDF, 6 pages, with tables and a chart | Quarterly risk disclosure of a fictional bank |
| `docs/q3_audit_summary.docx` | Word | Internal audit summary with findings, observations and a sign-off |
| `docs/regulatory_guidelines_2026.epub` | EPUB | Regulatory guidelines in seven chapters |

All organisations, people, identifiers and figures in these documents are fictional and
were generated for this example.

Two fields appear in two documents. Comparing them is part of the exercise: check
whether the values agree, and which document Bob took each one from.

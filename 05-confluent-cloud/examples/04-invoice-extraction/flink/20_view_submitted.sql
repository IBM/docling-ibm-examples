-- The submissions, with THE EXTRACTION TARGET attached — the one place it is.
--
--     submissions ──▶ submitted ──▶ extracted (synthetic | real) ──▶ …
--
-- Read from where the topic ended when the pipeline was deployed
-- (`${SUBMISSIONS_FROM_OFFSET}`, set by ./pipeline.sh deploy): a redeploy
-- extracts only what is released from then on, never an invoice twice — the
-- signed URLs on older ones have expired after an hour anyway. What was
-- extracted before comes back from the `extractions` topic instead (see
-- 22_view_extracted_all.sql). `event_time` is the submission's Kafka
-- timestamp — its simulated time.
--
-- THE TARGET is Docling's `ExtractionTarget` (docling-project/docling#4218):
--
--   output_schema  a JSON Schema — the contract. Docling validates every page's
--                  answer against it and reports validation_status. This one is
--                  what pydantic's model_json_schema() emits for an Invoice
--                  with a list of LineItem. (`format: date` is not enforced:
--                  the model writes dates as printed and they pass. invoice()
--                  parses them.)
--   template       guidance for the model. `example_json`: one made-up invoice,
--                  shown to the model as an example of the answer, and nothing
--                  more — no constraint. Docling for IBM watsonx runs a generic
--                  vision model (`granite_vision_v4_kserve`, the only preset it
--                  allows), which refuses NuExtract's native `nuextract`
--                  template; scored side by side, schema + example did a little
--                  better than schema alone (scripts/extract.py).
--   instructions   free text for the model.
--
-- Edit it and `./pipeline.sh deploy`: the pipeline statement is replaced (the
-- view is part of its fingerprint) and extracts what is released from then on
-- with the new target — never what came before. (No single quotes inside the literal: it
-- is a SQL string. The UDF tests and scripts/extract.py read it from here.)
CREATE VIEW `${VIEW_SUBMITTED}` AS
SELECT
  `key`,
  `doc_id`,
  `url`,
  `department`,
  `category`,
  `submitted_at`,
  COALESCE(`mode`, 'synthetic') AS `mode`,
  `$rowtime`                     AS `event_time`,
  '{
      "output_schema": {
        "title": "Invoice",
        "type": "object",
        "properties": {
          "vendor_name":    {"type": "string", "description": "Legal name of the company that issued the invoice (the seller, not the customer)"},
          "vendor_vat_id":  {"anyOf": [{"type": "string"}, {"type": "null"}], "description": "The seller VAT or tax registration number"},
          "invoice_number": {"type": "string", "description": "The invoice identifier, copied exactly"},
          "invoice_date":   {"type": "string", "format": "date", "description": "Date of issue"},
          "currency":       {"type": "string", "pattern": "^[A-Z]{3}$", "description": "ISO 4217 code of the invoice currency"},
          "subtotal":       {"anyOf": [{"type": "number"}, {"type": "null"}], "description": "Net amount before tax"},
          "tax_amount":     {"anyOf": [{"type": "number"}, {"type": "null"}], "description": "Total VAT or sales tax"},
          "total_amount":   {"type": "number", "description": "Final amount due, including tax"},
          "line_items":     {"type": "array", "items": {"$ref": "#/$defs/LineItem"}, "description": "One entry per billed line"}
        },
        "required": ["vendor_name", "invoice_number", "invoice_date", "currency", "total_amount"],
        "$defs": {
          "LineItem": {
            "title": "LineItem",
            "type": "object",
            "properties": {
              "description": {"type": "string"},
              "quantity":    {"anyOf": [{"type": "number"}, {"type": "null"}]},
              "unit_price":  {"anyOf": [{"type": "number"}, {"type": "null"}]},
              "amount":      {"type": "number"}
            },
            "required": ["description", "amount"]
          }
        }
      },
      "template": {
        "format": "example_json",
        "value": {
          "vendor_name": "Muster Software GmbH",
          "vendor_vat_id": "DE123456789",
          "invoice_number": "RE-2026-0042",
          "invoice_date": "2026-01-31",
          "currency": "EUR",
          "subtotal": 1000.0,
          "tax_amount": 190.0,
          "total_amount": 1190.0,
          "line_items": [{"description": "Support plan, January", "quantity": 1, "unit_price": 1000.0, "amount": 1000.0}]
        }
      },
      "instructions": "The vendor is the company that issued the invoice, not the bill-to customer. Write dates as YYYY-MM-DD and amounts as plain numbers, without currency symbols or thousands separators. Use the totals printed on the invoice; do not compute them."
    }' AS `extraction_target`
FROM `${SUBMISSIONS_TOPIC}` /*+ OPTIONS('scan.startup.mode' = 'specific-offsets',
  'scan.startup.specific-offsets' = 'partition:0,offset:${SUBMISSIONS_FROM_OFFSET}') */
WHERE `url` IS NOT NULL AND `url` <> '';

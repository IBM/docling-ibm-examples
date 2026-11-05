-- invoice(extraction, submission) -> one invoice record, as JSON.
--
-- Merges Docling's per-page items into one record (header fields from the
-- first page, totals from the last, line items from all), adds what the
-- submitter said — department, category, submission time — and decides
-- `status`: "ok", or "review" with `review_reasons`. The SQL routes on it.
--
-- This one stays when the fake goes: it is the pipeline's logic, not Docling's.
CREATE FUNCTION `${FN_INVOICE}`
  AS 'invoice_udf.functions.invoice'
  LANGUAGE PYTHON
  USING JAR 'confluent-artifact://${ARTIFACT_ID}/${ARTIFACT_VERSION}';

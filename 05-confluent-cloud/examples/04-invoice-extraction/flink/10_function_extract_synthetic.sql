-- extract_synthetic(docling_conn, artifact_conn, url, extraction_target)
--   -> Docling's ExtractionDocumentResult, as JSON — answered from the ground truth.
--
-- `invoice_udf.functions.synthetic_extract` never sees the PDF: it answers
-- from the truth compiled into the Python artifact (udf/compile_truth.py), in
-- the shape the real call answers in, as observed against Docling for IBM
-- watsonx — one item per page, dates as printed, per-page validation,
-- failures as values. Free and instant; what `mode = 'synthetic'` gets.
--
-- Same signature as the real one, connection names included (ignored), so the
-- two branches of flink/21_view_extracted.sql differ in the function name and
-- the filter only.
CREATE FUNCTION `${FN_EXTRACT_SYNTHETIC}`
  AS 'invoice_udf.functions.synthetic_extract'
  LANGUAGE PYTHON
  USING JAR 'confluent-artifact://${ARTIFACT_ID}/${ARTIFACT_VERSION}';

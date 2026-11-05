-- extract_real(docling_conn, artifact_conn, url, extraction_target)
--   -> Docling's ExtractionDocumentResult, as JSON. The Docling call.
--
-- `ai.docling.confluent.DoclingExtract`, in the Java artifact: submits the
-- target and the PDF's URL to Docling's extraction API
-- (docling-project/docling#4218), polls the task, fetches the presigned result
-- and returns it as Docling wrote it. Retries a task the service failed as
-- retryable, or a page whose model call timed out; any other failure is a
-- failure envelope, never an exception. See common/udf-java/.
--
-- An AsyncScalarFunction: several extractions in flight, results in input
-- order, and an answer within 45 s of each record — Confluent Cloud fails the
-- statement when an async call passes ~60 s.
--
-- USING CONNECTIONS makes both hosts reachable and their endpoint and token
-- visible to the function. Their names are also its first two arguments,
-- because every name here is derived from NS.
CREATE FUNCTION `${FN_EXTRACT_REAL}`
  AS 'ai.docling.confluent.DoclingExtract'
  USING JAR 'confluent-artifact://${JAVA_ARTIFACT_ID}/${JAVA_ARTIFACT_VERSION}'
  USING CONNECTIONS (`${DOCLING_CONNECTION}`, `${DOWNLOAD_CONNECTION}`);

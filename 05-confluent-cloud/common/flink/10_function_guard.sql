-- dc_guard(payload, detectors, redact, min_chars, drop_low_quality, blocked)
--   -> the guarded record, with pii_count / pii_types / rejected_reason set.
--
-- A UDF has no state and no side outputs, so the policy arrives as arguments
-- and the two side outputs become fields the SQL routes on. Changing a rule
-- means editing .env and redeploying; docs/05-confluent-cloud/LIMITS.md has a temporal-join sketch
-- that would make it live.
CREATE FUNCTION `${FN_GUARD}`
  AS 'docling_udf.functions.dc_guard'
  LANGUAGE PYTHON
  USING JAR 'confluent-artifact://${ARTIFACT_ID}/${ARTIFACT_VERSION}';

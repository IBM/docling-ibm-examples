-- The pipeline.
--
--     chunks -> prepare -> guard --+--> quarantine   (the PII originals)
--                                  +--> rejected     (what the gate dropped)
--                                  |
--                                  +--> dedup -> enriched
--
-- Three INSERTs in one statement set, which is how Flink SQL spells a side
-- output: one job, one source, one evaluation of each UDF, three sinks, with
-- the routing in WHERE clauses.
--
-- ENCODE on the way out mirrors the DECODE in 20_view_prepared.sql: the topics
-- are raw, so both `key` and `val` are BYTES, and the records travel between
-- the UDFs as STRING.
--
-- There is no embedding stage: a Confluent Cloud Python UDF cannot make a
-- network call, so watsonx.ai is not reachable from inside the statement.
-- docs/05-confluent-cloud/LIMITS.md lists three ways to add embeddings; `AI_EMBEDDING` over a
-- Confluent managed model is the one that needs no other account.
--
-- Dedup sits on the main branch only, after the side outputs. A duplicate that
-- carried PII therefore still reaches quarantine twice, which is the intended
-- behaviour: quarantine is an audit of deliveries, not of distinct chunks.
--
-- The dedup is Flink SQL's own deduplication pattern: ROW_NUMBER() partitioned
-- by the fingerprint, ordered by the record time, keeping row 1. Flink
-- recognises that shape and runs its Deduplicate operator, and the state's
-- lifetime is the `sql.state-ttl` property the statement is submitted with
-- (see `.env`), not a descriptor in the code.
EXECUTE STATEMENT SET
BEGIN

-- 1. the main stream: guarded, deduplicated, into the enriched topic.
INSERT INTO `${ENRICHED_TOPIC}`
SELECT
  ENCODE(JSON_VALUE(`guarded`, '$.chunk_id'), 'UTF-8') AS `key`,
  ENCODE(`guarded`, 'UTF-8')                           AS `val`
FROM (
  SELECT
    `guarded`,
    ROW_NUMBER() OVER (
      PARTITION BY JSON_VALUE(`guarded`, '$.fingerprint')
      ORDER BY `event_time`
    ) AS `rn`
  FROM `${VIEW_GUARDED}`
  WHERE JSON_VALUE(`guarded`, '$.rejected_reason') IS NULL
)
WHERE `rn` = 1;

-- 2. quarantine: the ORIGINAL of anything the detectors matched. Never
--    `guarded` — that one has the values replaced, which is the whole point.
INSERT INTO `${QUARANTINE_TOPIC}`
SELECT
  ENCODE(JSON_VALUE(`original`, '$.chunk_id'), 'UTF-8') AS `key`,
  ENCODE(`original`, 'UTF-8')                           AS `val`
FROM `${VIEW_GUARDED}`
WHERE CAST(JSON_VALUE(`guarded`, '$.pii_count') AS INT) > 0;

-- 3. rejected: the doc-id block list and the quality gate, kept visible. A gate
--    whose drops go nowhere is indistinguishable from a broken pipeline.
INSERT INTO `${REJECTED_TOPIC}`
SELECT
  ENCODE(JSON_VALUE(`guarded`, '$.chunk_id'), 'UTF-8') AS `key`,
  ENCODE(`guarded`, 'UTF-8')                           AS `val`
FROM `${VIEW_GUARDED}`
WHERE JSON_VALUE(`guarded`, '$.rejected_reason') IS NOT NULL;

END;

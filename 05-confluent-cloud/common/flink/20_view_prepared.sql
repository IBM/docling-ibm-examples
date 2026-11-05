-- The pipeline is three sinks off one stream, so it is built as two views and
-- one statement set rather than as three copies of the same query. A
-- statement set plans its INSERTs together and reuses the shared sub-plan, so
-- dc_prepare and dc_guard are each evaluated once per record even though three
-- branches read them.
--
-- View 1 — prepare, with the quality gate deliberately OFF
-- (`FALSE` below, not ${DROP_LOW_QUALITY}). The gate belongs to the guard,
-- because there the threshold is policy rather than a start-up value; prepare
-- only flags.
--
-- `$rowtime` is Confluent Cloud's system column for the Kafka record timestamp.
-- It is carried through both views because the dedup below has to ORDER BY a
-- time attribute — a plain column there is rejected at plan time.
CREATE VIEW `${VIEW_PREPARED}` AS
SELECT
  `prepared`,
  `event_time`
FROM (
  SELECT
    ${FN_PREPARE}(`payload`, ${MIN_CHARS}, FALSE) AS `prepared`,
    `event_time`
  FROM (
    -- The one place the topic's bytes become text. Decoding here rather than
    -- at each call site means one DECODE per record instead of two, and it
    -- keeps the UDF arguments plainly STRING.
    SELECT
      DECODE(`${CHUNKS_VALUE_COLUMN}`, 'UTF-8') AS `payload`,
      `$rowtime`                                AS `event_time`
    FROM `${CHUNKS_TOPIC}`
  )
  -- WHERE is evaluated before the projection, so the cheap gate still runs
  -- before ${FN_PREPARE} does.
  WHERE ${FN_IS_CHUNK}(`payload`)
)
WHERE `prepared` <> '';

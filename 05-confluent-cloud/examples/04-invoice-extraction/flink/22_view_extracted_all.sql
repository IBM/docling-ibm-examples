-- Every invoice's extraction the reports are computed from — the latest one
-- per invoice: those of earlier deploys, from the `extractions` topic, and the
-- new ones, as they happen.
--
--     extractions (up to the deploy) ──┐
--                                      ├─▶ latest per doc_id ─▶ extracted_all ─▶ invoiced ─▶ …
--     extracted   (from the deploy) ───┘
--
-- The topic is read *bounded*: up to where it ended when the pipeline was
-- deployed (`${EXTRACTIONS_UNTIL_OFFSET}`), and not past it — past it are this
-- statement's own writes, which arrive here directly. So a redeploy rebuilds
-- every report from what was already extracted, for free, and adds the new
-- extractions without a Kafka hop between the extraction and the reports.
--
-- One row per invoice: an invoice released twice (two clocks, a re-release)
-- or extracted again replaces its earlier extraction — retracted from every
-- report downstream — instead of being counted twice. `arrived` — when the row
-- reached this statement (CURRENT_TIMESTAMP, per row in a streaming query;
-- Confluent has no PROCTIME()) — orders them, so the newest wins.
CREATE VIEW `${VIEW_EXTRACTED_ALL}` AS
SELECT `key`, `doc_id`, `url`, `department`, `category`, `submitted_at`, `mode`, `extraction`, `event_time`
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY `doc_id` ORDER BY `arrived` DESC) AS `rn`
  FROM (
    SELECT `key`, `doc_id`, `url`, `department`, `category`, `submitted_at`, `mode`, `extraction`, `event_time`,
           CURRENT_TIMESTAMP AS `arrived`
    FROM `${EXTRACTIONS_TOPIC}` /*+ OPTIONS('scan.bounded.mode' = 'specific-offsets',
      'scan.bounded.specific-offsets' = 'partition:0,offset:${EXTRACTIONS_UNTIL_OFFSET}') */
    UNION ALL
    SELECT `key`, `doc_id`, `url`, `department`, `category`, `submitted_at`, `mode`, `extraction`, `event_time`,
           CURRENT_TIMESTAMP AS `arrived`
    FROM `${VIEW_EXTRACTED}`
  )
)
WHERE `rn` = 1;

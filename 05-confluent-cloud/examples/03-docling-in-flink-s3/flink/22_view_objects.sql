-- The objects to convert: one row per upload, from the S3 events.
--
--     objects topic ──▶ Body ──▶ Records[0] ──▶ {bucket, key, etag, size}
--
-- Two rules about S3 events rather than about Flink:
--
-- 1. **The key is URL-encoded.** S3 writes `inbox/my report.pdf` as
--    `inbox/my+report.pdf` in an event — form encoding, `+` for a space — and
--    URL_DECODE undoes exactly that. dc_presign wants the real key; signing
--    the encoded one is a 403 that names nothing.
--
-- 2. **Not every message is an upload.** Creating the notification sends one
--    `s3:TestEvent`, which has no Records, and a bucket can be configured to
--    send deletes. `eventName LIKE 'ObjectCreated:%'` keeps real uploads and
--    scripts/backfill.py's `ObjectCreated:Backfill`, which says where it came
--    from without failing the test. Folder placeholders (size 0, a trailing
--    `/`) go too.
--
-- ## Why there is no dedup here
--
-- The obvious one — ROW_NUMBER() over (bucket, key, etag), keep the first — is
-- not usable. Ordered by $rowtime, Flink holds each row until the watermark
-- passes it, and an idle bucket has no watermark worth the name: the last
-- upload would wait for the next one (docs/05-confluent-cloud/LIMITS.md, "the dedup holds rows
-- until the watermark moves"). Ordered by arrival it would emit at once, but
-- PROCTIME() is one of the things Confluent Cloud does not run.
--
-- So duplicates are kept out at the source instead. The backfill sends only
-- objects older than the notification (S3_NOTIFIED_AT, written by
-- scripts/aws.sh create), so the two roads into the queue do not overlap.
-- What is left is SQS's at-least-once delivery, which is rare on a standard
-- queue, costs one extra conversion when it happens, and whose chunks the
-- pipeline's own dedup collapses — they have the same fingerprint.
--
-- Read from where the topic ended when the convert statement was deployed
-- (`${OBJECTS_FROM_OFFSETS}`), exactly as example 02 reads its URLs.
CREATE VIEW `${VIEW_OBJECTS}` AS
SELECT `bucket`, `key`, `etag`, `size`, `event_name`, `event_time`
FROM (
  SELECT
    JSON_VALUE(`event`, '$.Records[0].s3.bucket.name')                      AS `bucket`,
    URL_DECODE(JSON_VALUE(`event`, '$.Records[0].s3.object.key'))           AS `key`,
    JSON_VALUE(`event`, '$.Records[0].s3.object.eTag')                      AS `etag`,
    JSON_VALUE(`event`, '$.Records[0].s3.object.size' RETURNING BIGINT)     AS `size`,
    JSON_VALUE(`event`, '$.Records[0].eventName')                           AS `event_name`,
    JSON_VALUE(`event`, '$.Records[0].eventTime')                           AS `event_time`
  FROM (
    SELECT
      -- The connector's struct field is `Body`; `body` is accepted too, so
      -- that a lower-casing converter setting does not make every row vanish.
      COALESCE(JSON_VALUE(`payload`, '$.Body'), JSON_VALUE(`payload`, '$.body')) AS `event`
    FROM (
      SELECT DECODE(`val`, 'UTF-8') AS `payload`
      FROM `${OBJECTS_TOPIC}` /*+ OPTIONS('scan.startup.mode' = 'specific-offsets',
        'scan.startup.specific-offsets' = '${OBJECTS_FROM_OFFSETS}') */
    )
  )
)
WHERE `event_name` LIKE 'ObjectCreated:%'
  -- The bucket connection signs for one bucket; an event from another one
  -- would be presigned against the wrong host.
  AND `bucket` = '${S3_BUCKET}'
  AND `key` LIKE '${S3_PREFIX}%'
  AND `key` NOT LIKE '%/'
  AND COALESCE(`size`, 1) > 0;

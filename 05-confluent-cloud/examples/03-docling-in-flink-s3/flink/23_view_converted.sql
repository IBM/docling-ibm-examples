-- One Docling conversion per new object: presign it, hand Docling the URL.
--
--     objects ──▶ dc_presign(key) ──▶ dc_convert(presigned url) ──▶ converted
--
-- The only new idea in this example is the first arrow. A presigned URL is
-- an ordinary https:// URL that carries its own short-lived credential, so
-- from dc_convert onwards nothing knows the document was private — it is
-- example 02's call, with example 02's failure rows.
--
-- What the rows are called downstream: `doc_id` is the object key, which is
-- unique in the bucket and stable when nothing moves; `url` is the s3:// URI,
-- which is what a reader of the chunks wants to see — never the presigned
-- URL, which is long, expires, and is not the document's name.
--
-- A view for the same reason as in example 02: 31_convert.sql reads it twice,
-- and the statement set's shared sub-plan is what keeps that to one billed
-- conversion — and, here, one signature — per object.
CREATE VIEW `${VIEW_CONVERTED}` AS
SELECT
  `doc_id`,
  `url`,
  ${FN_CONVERT}(
    '${DOCLING_CONNECTION}',
    '${DOWNLOAD_CONNECTION}',
    ${FN_PRESIGN}('${BUCKET_CONNECTION}', `key`, ${PRESIGN_TTL}),
    '${DOCLING_FORMAT}'
  ) AS `converted`
FROM (
  SELECT
    `key`,
    `key`                                    AS `doc_id`,
    CONCAT('s3://', `bucket`, '/', `key`)    AS `url`
  FROM `${VIEW_OBJECTS}`
);

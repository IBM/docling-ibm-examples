-- View 2 — guard, and the reason it keeps two columns.
--
-- `original` is the record as prepare left it, PII and all. `guarded` is the
-- same record after redaction. The quarantine sink writes `original`, because
-- an audit trail of redacted text is not an audit trail.
--
-- Keep this topic's ACLs tight. It is the one place the matched values survive.
CREATE VIEW `${VIEW_GUARDED}` AS
SELECT
  `prepared` AS `original`,
  ${FN_GUARD}(
    `prepared`,
    '${PII_DETECTORS}',
    ${PII_REDACT},
    ${MIN_CHARS},
    ${DROP_LOW_QUALITY},
    '${BLOCKED_DOC_IDS}'
  ) AS `guarded`,
  `event_time`
FROM `${VIEW_PREPARED}`;

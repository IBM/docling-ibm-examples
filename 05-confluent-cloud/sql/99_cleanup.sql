-- 5. Clean up. First stop the long-running statement (the EXECUTE STATEMENT
--    SET from 02_pipeline.sql) in the Flink statements list: it is what uses
--    compute (CFUs). Then drop the views and tables. Dropping a table also
--    deletes its topic and data.

DROP VIEW `docling_guarded`;
DROP VIEW `docling_prepared`;

DROP TABLE `docling_enriched`;
DROP TABLE `docling_quarantine`;
DROP TABLE `docling_rejected`;
DROP TABLE `docling_chunks`;

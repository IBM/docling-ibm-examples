-- Deletes the topic `${QUARANTINE_TOPIC}` AND EVERYTHING IN IT. This one is the PII audit trail — the only copy of the originals.
--
-- In Confluent Cloud a Flink table is a Kafka topic, so DROP TABLE is not a
-- metadata operation — the data goes with it, and it does not come back. Run
-- only through `./setup.sh destroy --topics`, which says what it is about to
-- delete and makes you type the word.
DROP TABLE IF EXISTS `${QUARANTINE_TOPIC}`;

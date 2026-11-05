-- Deletes the topic `${OBJECTS_TOPIC}` AND EVERYTHING IN IT. This one is the
-- object notifications the connector wrote — scripts/backfill.py writes them
-- again from the bucket.
--
-- In Confluent Cloud a Flink table is a Kafka topic, so DROP TABLE is not a
-- metadata operation — the data goes with it, and it does not come back. Run
-- only through `./setup.sh destroy --topics`, which says what it is about to
-- delete and makes you type the words. Destroy deletes the connector first, so
-- nothing recreates the topic behind it.
DROP TABLE IF EXISTS `${OBJECTS_TOPIC}`;

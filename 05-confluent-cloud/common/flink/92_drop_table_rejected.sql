-- Deletes the topic `${REJECTED_TOPIC}` AND EVERYTHING IN IT. This one is what the quality gate dropped.
--
-- In Confluent Cloud a Flink table is a Kafka topic, so DROP TABLE is not a
-- metadata operation — the data goes with it, and it does not come back. Run
-- only through `./setup.sh destroy --topics`, which says what it is about to
-- delete and makes you type the word.
DROP TABLE IF EXISTS `${REJECTED_TOPIC}`;

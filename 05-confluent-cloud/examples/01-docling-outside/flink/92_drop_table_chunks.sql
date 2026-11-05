-- Deletes the topic `${CHUNKS_TOPIC}` AND EVERYTHING IN IT. This one is Docling's. Hours of conversion may be behind it.
--
-- In Confluent Cloud a Flink table is a Kafka topic, so DROP TABLE is not a
-- metadata operation — the data goes with it, and it does not come back. Run
-- only through `./setup.sh destroy --topics`, which says what it is about to
-- delete and makes you type the word.
DROP TABLE IF EXISTS `${CHUNKS_TOPIC}`;

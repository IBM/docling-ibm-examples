-- Deletes the topic `${URLS_TOPIC}` AND EVERYTHING IN IT. This one is the list
-- of documents to convert — cheap to write again.
--
-- In Confluent Cloud a Flink table is a Kafka topic, so DROP TABLE is not a
-- metadata operation — the data goes with it, and it does not come back. Run
-- only through `./setup.sh destroy --topics`, which says what it is about to
-- delete and makes you type the words.
DROP TABLE IF EXISTS `${URLS_TOPIC}`;

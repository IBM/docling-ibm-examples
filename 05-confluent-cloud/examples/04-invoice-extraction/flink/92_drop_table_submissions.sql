-- Deletes the topic `${SUBMISSIONS_TOPIC}` AND EVERYTHING IN IT. In Confluent Cloud a
-- Flink table is a Kafka topic. Run only through `./setup.sh destroy --topics`,
-- which is also how the simulation is reset to day one.
DROP TABLE IF EXISTS `${SUBMISSIONS_TOPIC}`;

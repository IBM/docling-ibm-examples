-- Deletes the topic `${FX_TOPIC}` AND EVERYTHING IN IT. In Confluent Cloud a
-- Flink table is a Kafka topic. Run only through `./setup.sh destroy --topics`,
-- which is also how the simulation is reset to day one.
DROP TABLE IF EXISTS `${FX_TOPIC}`;

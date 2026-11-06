-- Deletes the topic `${DAILY_TOPIC}` AND EVERYTHING IN IT. Run only through
-- `./setup.sh destroy --topics`.
DROP TABLE IF EXISTS `${DAILY_TOPIC}`;

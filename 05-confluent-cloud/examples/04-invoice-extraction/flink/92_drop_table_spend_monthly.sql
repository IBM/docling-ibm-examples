-- Deletes the topic `${MONTHLY_TOPIC}` AND EVERYTHING IN IT. Run only through
-- `./setup.sh destroy --topics`.
DROP TABLE IF EXISTS `${MONTHLY_TOPIC}`;

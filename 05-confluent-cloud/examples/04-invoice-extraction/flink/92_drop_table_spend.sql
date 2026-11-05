-- Deletes the topic `${SPEND_TOPIC}` AND EVERYTHING IN IT. Run only through
-- `./setup.sh destroy --topics`.
DROP TABLE IF EXISTS `${SPEND_TOPIC}`;

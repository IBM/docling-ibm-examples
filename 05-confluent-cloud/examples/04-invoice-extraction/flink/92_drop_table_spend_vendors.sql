-- Deletes the topic `${VENDORS_TOPIC}` AND EVERYTHING IN IT. Run only through
-- `./setup.sh destroy --topics`.
DROP TABLE IF EXISTS `${VENDORS_TOPIC}`;

-- testing
-- SELECT id, bversion, ip,
-- TIMESTAMP 'epoch' + last_seen_ts * INTERVAL '1 second' as date_last_seen
-- FROM devices;

ALTER TABLE devices
ALTER COLUMN last_seen_ts TYPE timestamp
USING TIMESTAMP 'epoch' + last_seen_ts * INTERVAL '1 second';
ALTER TABLE devices
RENAME COLUMN last_seen_ts to date_last_seen;

ALTER TABLE devices_log
ALTER COLUMN ts TYPE timestamp
USING TIMESTAMP 'epoch' + ts * INTERVAL '1 second';
ALTER TABLE devices_log
RENAME COLUMN ts to date_seen;

-- testing
-- SELECT id, fqdn,
-- TIMESTAMP 'epoch' + free_ts * INTERVAL '1 second' as date_free, curr_cli,
-- max_cli, available
-- FROM targets;

ALTER TABLE targets
ALTER COLUMN free_ts TYPE timestamp
USING TIMESTAMP 'epoch' + free_ts * INTERVAL '1 second';
ALTER TABLE targets
RENAME COLUMN free_ts to date_free;

-- testing
-- SELECT device_id, port,
-- TIMESTAMP 'epoch' + created_ts * INTERVAL '1 second' as date_created
-- FROM tunnels;

ALTER TABLE tunnels
ALTER COLUMN created_ts TYPE timestamp
USING TIMESTAMP 'epoch' + created_ts * INTERVAL '1 second';
ALTER TABLE tunnels
RENAME COLUMN created_ts to date_created;

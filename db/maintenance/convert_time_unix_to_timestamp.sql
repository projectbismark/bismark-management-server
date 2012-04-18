BEGIN;

-- devices
ALTER TABLE devices ALTER COLUMN last_seen_ts TYPE timestamp
USING TIMESTAMP 'epoch' + last_seen_ts * INTERVAL '1 second';

ALTER TABLE devices RENAME COLUMN last_seen_ts to date_last_seen;

-- devices_log
ALTER TABLE devices_log ALTER COLUMN ts TYPE timestamp
USING TIMESTAMP 'epoch' + ts * INTERVAL '1 second';

ALTER TABLE devices_log RENAME COLUMN ts to date_seen;

CREATE OR REPLACE function log_probe() RETURNS trigger as
$log_probe$
	BEGIN
		EXECUTE 'INSERT INTO devices_log '
			|| ' (id,bversion,ip,date_seen) VALUES ('
			|| ' $1,$2,$3,$4)'
			USING NEW.id,NEW.bversion,NEW.ip,NEW.date_last_seen;
		RETURN NEW;
	END;
$log_probe$
LANGUAGE plpgsql;

-- targets
ALTER TABLE targets ALTER COLUMN free_ts TYPE timestamp
USING TIMESTAMP 'epoch' + free_ts * INTERVAL '1 second';

ALTER TABLE targets RENAME COLUMN free_ts to date_free;

-- tunnels
ALTER TABLE tunnels ALTER COLUMN created_ts TYPE timestamp
USING TIMESTAMP 'epoch' + created_ts * INTERVAL '1 second';

ALTER TABLE tunnels RENAME COLUMN created_ts to date_created;

COMMIT;

/* TODO: Check for error on insert*/
CREATE LANGUAGE plpgsql;
CREATE OR REPLACE function log_probe() RETURNS trigger as
$log_probe$
	BEGIN
		EXECUTE 'INSERT INTO devices_log '
			|| ' (id,bversion,ip,ts) VALUES ('
			|| ' $1,$2,$3,$4)'
			USING NEW.id,NEW.bversion,NEW.ip,NEW.last_seen_ts;
		RETURN NEW;
	END;
$log_probe$
LANGUAGE plpgsql;

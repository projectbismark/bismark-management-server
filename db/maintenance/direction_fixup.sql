CREATE TEMP TABLE mserver_ips ( ip inet );
INSERT INTO mserver_ips VALUES ('4.71.254.153');
INSERT INTO mserver_ips VALUES ('4.71.254.140');
INSERT INTO mserver_ips VALUES ('216.156.197.146');
INSERT INTO mserver_ips VALUES ('143.215.131.173');

-- AGGL3BITRATE is currently screwed up
--
-- BEGIN;
-- UPDATE m_aggl3bitrate
-- SET direction = 'up'
-- WHERE direction is NULL
-- AND dstip IN (
--     SELECT ip FROM mserver_ips
-- );
-- UPDATE m_aggl3bitrate
-- SET direction = 'dw'
-- WHERE direction is NULL
-- AND srcip IN (
--     SELECT ip FROM mserver_ips
-- )
-- COMMIT;

BEGIN;
UPDATE m_bitrate
SET direction = 'up'
WHERE direction is NULL
AND dstip IN (
    SELECT ip FROM mserver_ips
);
UPDATE m_bitrate
SET direction = 'dw'
WHERE direction is NULL
AND srcip IN (
    SELECT ip FROM mserver_ips
);
COMMIT;

BEGIN;
UPDATE m_capacity
SET direction = 'up'
WHERE direction is NULL
AND dstip IN (
    SELECT ip FROM mserver_ips
);
UPDATE m_capacity
SET direction = 'dw'
WHERE direction is NULL
AND srcip IN (
    SELECT ip FROM mserver_ips
);
COMMIT;

BEGIN;
UPDATE m_jitter
SET direction = 'up'
WHERE direction is NULL
AND dstip IN (
    SELECT ip FROM mserver_ips
);
UPDATE m_jitter
SET direction = 'dw'
WHERE direction is NULL
AND srcip IN (
    SELECT ip FROM mserver_ips
);
COMMIT;

BEGIN;
UPDATE m_pktloss
SET direction = 'up'
WHERE direction is NULL
AND dstip IN (
    SELECT ip FROM mserver_ips
);
UPDATE m_pktloss
SET direction = 'dw'
WHERE direction is NULL
AND srcip IN (
    SELECT ip FROM mserver_ips
);
COMMIT;

BEGIN;
UPDATE m_shaperate
SET direction = 'up'
WHERE direction is NULL
AND dstip IN (
    SELECT ip FROM mserver_ips
);
UPDATE m_shaperate
SET direction = 'dw'
WHERE direction is NULL
AND srcip IN (
    SELECT ip FROM mserver_ips
);
COMMIT;

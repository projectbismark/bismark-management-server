CREATE SCHEMA public;

CREATE DOMAIN fqdn_t        AS varchar(255);    -- fully-qualified domain name
CREATE DOMAIN id_t          AS varchar(50);     -- must allow 'BDM'
CREATE DOMAIN ip_t          AS inet;
CREATE DOMAIN msg_t         AS varchar(100);
CREATE DOMAIN svcinfo_t     AS varchar(500);
CREATE DOMAIN svcname_t     AS varchar(50);
CREATE DOMAIN version_t     AS varchar(50);

CREATE TABLE devices (
    id              id_t            PRIMARY KEY,
    bversion        version_t       NOT NULL,
    ip              ip_t            NOT NULL,
    date_last_seen  timestamp       NOT NULL
);

CREATE TABLE devices_log (
    id              id_t            NOT NULL,
    bversion        version_t       NOT NULL,
    ip              ip_t            NOT NULL,
    date_seen       timestamp       NOT NULL
);

-- log device check-ins in device_log
CREATE TRIGGER log_probe AFTER UPDATE on devices FOR EACH ROW
    EXECUTE PROCEDURE log_probe();

CREATE TABLE tunnels (
    device_id       id_t            NOT NULL REFERENCES devices (id),
    port            integer         NOT NULL,
    date_created    timestamp       NOT NULL,
    PRIMARY KEY (device_id, port)
);

CREATE TABLE messages (
    id              serial          PRIMARY KEY,
    msgfrom         id_t            NOT NULL,
    msgto           id_t            NOT NULL,
    msg             msg_t           NOT NULL
);

CREATE TABLE targets (
    id              serial          NOT NULL UNIQUE,
    fqdn            fqdn_t          PRIMARY KEY,
    date_free       timestamp       NOT NULL DEFAULT NOW(),
    curr_cli        integer         NOT NULL,
    max_cli         integer         NOT NULL,
    available       boolean         NOT NULL DEFAULT FALSE
);

CREATE TABLE device_targets (
    device_id       id_t            NOT NULL REFERENCES devices (id),
    target_id       integer         NOT NULL REFERENCES targets (id),
    preference      integer         NOT NULL DEFAULT 0,
    is_enabled      boolean         NOT NULL DEFAULT FALSE,
    -- date_effective, is_permanent are only used by update_device_targets.py
    date_effective  timestamp       NOT NULL DEFAULT now(),
    is_permanent    boolean         NOT NULL DEFAULT FALSE,
    PRIMARY KEY (device_id, target_id, date_effective)
);

CREATE TABLE target_ips (
    target_id       integer         NOT NULL REFERENCES targets (id),
    ip              ip_t            NOT NULL,
    date_effective  timestamp       NOT NULL
);

CREATE TABLE target_services (
    target_id       integer         NOT NULL REFERENCES targets (id),
    service_id      integer         NOT NULL REFERENCES services (id),
    info            svcinfo_t       NOT NULL,
    PRIMARY KEY (target_id, service_id)
);

CREATE TABLE services (
    id              serial          NOT NULL UNIQUE,
    name            svcname_t       PRIMARY KEY,
    is_exclusive    boolean         NOT NULL
);

CREATE TABLE blacklist (
    device_id       id_t            PRIMARY KEY
);

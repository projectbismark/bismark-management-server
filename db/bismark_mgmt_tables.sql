CREATE SCHEMA public;

CREATE DOMAIN id_t AS varchar(50);
CREATE DOMAIN version_t AS varchar(50);
CREATE DOMAIN user_t AS varchar(50);
CREATE DOMAIN ip_t AS inet;
CREATE DOMAIN ts_t AS integer;
CREATE DOMAIN cat_t AS varchar(50);
CREATE DOMAIN msg_from_t AS varchar(50);
CREATE DOMAIN msg_to_t AS varchar(50);
CREATE DOMAIN msg_t AS varchar(100);
CREATE DOMAIN zone_t AS varchar(50);
CREATE DOMAIN cli_t AS integer;
CREATE DOMAIN prio_t AS integer;
CREATE DOMAIN info_t AS varchar(500);
CREATE DOMAIN mtype_t AS varchar(50);

CREATE TABLE devices (
    id          id_t        PRIMARY KEY,
    bversion    version_t,
    duser       user_t,
    ip          ip_t,
    ts          ts_t
);

CREATE TABLE tunnels (
    device_id   id_t,
    port        integer,
    ts          ts_t,
    PRIMARY KEY (device_id, port)
);

CREATE TABLE messages (
    rowid       serial      PRIMARY KEY,
    msgfrom     msg_from_t,
    msgto       msg_to_t,
    msg         msg_t
);

CREATE TABLE targets (
    ip          ip_t        PRIMARY KEY,
    cat         cat_t,
    zone        zone_t,
    free_ts     ts_t        DEFAULT 0,
    curr_cli    cli_t,
    max_cli     cli_t,
    available   boolean     DEFAULT FALSE
);

CREATE TABLE device_targets (
    device_id   id_t,
    target_ip   ip_t,
    priority    prio_t      DEFAULT 0,
    PRIMARY KEY (device_id, target_ip)
);

CREATE TABLE capabilities (
    target_ip   ip_t,
    service     mtype_t,
    info        info_t,
    PRIMARY KEY (target_ip, service)
);

CREATE TABLE mtypes (
    mtype       mtype_t     PRIMARY KEY,
    mexclusive  boolean
);

CREATE TABLE blacklist (
    device_id   id_t        PRIMARY KEY
);


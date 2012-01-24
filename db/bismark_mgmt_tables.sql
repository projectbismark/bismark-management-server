CREATE SCHEMA public;

CREATE DOMAIN id_t AS varchar(50);
CREATE DOMAIN version_t AS varchar(50);
CREATE DOMAIN user_t AS varchar(50);
CREATE DOMAIN ip_t AS inet;
CREATE DOMAIN ts_t AS integer;
CREATE DOMAIN cat_t AS varchar(50);
CREATE DOMAIN msg_addr_t AS varchar(50);
CREATE DOMAIN msg_t AS varchar(100);
CREATE DOMAIN zone_t AS varchar(50);
CREATE DOMAIN cli_t AS integer;
CREATE DOMAIN prio_t AS integer;
CREATE DOMAIN info_t AS varchar(500);
CREATE DOMAIN mtype_t AS varchar(50);

CREATE TABLE devices (
    id          id_t        PRIMARY KEY,
    bversion    version_t   NOT NULL,
    duser       user_t      NOT NULL,
    ip          ip_t        NOT NULL,
    ts          ts_t        NOT NULL
);

CREATE TABLE tunnels (
    device_id   id_t        NOT NULL,
    port        integer     NOT NULL,
    ts          ts_t        NOT NULL,
    PRIMARY KEY (device_id, port)
);

CREATE TABLE messages (
    rowid       serial      PRIMARY KEY,
    msgfrom     msg_addr_t  NOT NULL,
    msgto       msg_addr_t  NOT NULL,
    msg         msg_t       NOT NULL
);

CREATE TABLE targets (
    ip          ip_t        PRIMARY KEY,
    cat         cat_t       NOT NULL,
    zone        zone_t      NOT NULL,
    free_ts     ts_t        NOT NULL DEFAULT 0,
    curr_cli    cli_t       NOT NULL,
    max_cli     cli_t       NOT NULL,
    available   boolean     NOT NULL DEFAULT FALSE
);

CREATE TABLE device_targets (
    device_id   id_t        NOT NULL,
    target_ip   ip_t        NOT NULL,
    priority    prio_t      NOT NULL DEFAULT 0,
    PRIMARY KEY (device_id, target_ip)
);

CREATE TABLE capabilities (
    target_ip   ip_t        NOT NULL,
    service     mtype_t     NOT NULL,
    info        info_t      NOT NULL,
    PRIMARY KEY (target_ip, service)
);

CREATE TABLE mtypes (
    mtype       mtype_t     PRIMARY KEY,
    mexclusive  boolean     NOT NULL
);

CREATE TABLE blacklist (
    device_id   id_t        PRIMARY KEY
);


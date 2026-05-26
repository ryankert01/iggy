-- Pre-create the database + table the Doris sink writes into.
-- The connector never issues DDL, so this must exist before it starts.
--
-- Column shape matches the JSON produced by produce.sh.
-- replication_num=1 / storage_medium=HDD because the all-in-one image has a
-- single HDD-backed BE (otherwise CREATE fails with "Failed to find enough
-- backend ... storage medium: SSD").

CREATE DATABASE IF NOT EXISTS iggy_demo;

CREATE TABLE IF NOT EXISTS iggy_demo.events (
    id        BIGINT      NOT NULL,
    name      VARCHAR(64) NOT NULL,
    count     INT         NOT NULL,
    amount    DOUBLE      NOT NULL,
    active    BOOLEAN     NOT NULL,
    timestamp BIGINT      NOT NULL
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "storage_medium"  = "HDD"
);

-- Read-only user WITH a password, purely so Adminer (http://localhost:8088)
-- will accept the login — the official Adminer image refuses blank passwords.
-- The connector keeps using root/blank; this user only browses.
CREATE USER IF NOT EXISTS 'viewer' IDENTIFIED BY 'viewer';
GRANT SELECT_PRIV ON iggy_demo.* TO 'viewer';
GRANT SELECT_PRIV ON information_schema.* TO 'viewer';

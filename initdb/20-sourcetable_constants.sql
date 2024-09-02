CREATE TABLE IF NOT EXISTS sourcetable_constants (
    mountpoint_id SERIAL,
    mountpoint VARCHAR(50),
    casterprovider VARCHAR(50),
    city VARCHAR(50),
    rtcmver VARCHAR(50),
    countrycode VARCHAR(50),
    latitude DECIMAL(7,4),
    longitude DECIMAL(7,4),
    receiver VARCHAR(50),
    UNIQUE (mountpoint, countrycode, casterprovider)
);

CREATE INDEX ON sourcetable_constants(mountpoint, countrycode, casterprovider DESC);
CREATE INDEX ON sourcetable_constants(mountpoint_id DESC);
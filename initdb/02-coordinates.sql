CREATE TABLE IF NOT EXISTS coordinates (
    obs_id BIGSERIAL,
    rtcm_package_id BIGSERIAL,
    rtcm_msg_type SMALLINT NOT NULL,
    mountpoint VARCHAR(50) UNIQUE,
    ecef_x NUMERIC(10, 3),
    ecef_y NUMERIC(10, 3),
    ecef_z NUMERIC(10, 3),
    antHgt NUMERIC(10, 3)
);

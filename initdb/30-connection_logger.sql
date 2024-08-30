CREATE TABLE IF NOT EXISTS connection_logger (
    id SERIAL PRIMARY KEY,
    mountpoint VARCHAR(50),
    disconnect_time TIMESTAMP WITHOUT TIME ZONE,
    reconnect_time TIMESTAMP WITHOUT TIME ZONE
);
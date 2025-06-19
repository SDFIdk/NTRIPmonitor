CREATE OR REPLACE FUNCTION public.insert_rtcm_packages(decodedframes json, OUT rtcmpackageids bigint[])
 RETURNS bigint[]
 LANGUAGE plpgsql
AS $$
BEGIN
    -- Insert into rtcm_packages and get the IDs
    WITH ins AS (
        INSERT INTO rtcm_packages (mountpoint, receive_time, rtcm_obs_epoch, rtcm_msg_type, rtcm_msg_size, rtcm_sat_count)
        SELECT (json_array_elements->>0)::text, 
               (json_array_elements->>1)::timestamp without time zone, 
               (json_array_elements->>2)::timestamp with time zone, 
               (json_array_elements->>3)::integer, 
               (json_array_elements->>4)::integer, 
               (json_array_elements->>5)::integer
        FROM json_array_elements(decodedFrames) 
        RETURNING rtcm_package_id
    )
    SELECT array_agg(rtcm_package_id) FROM ins INTO rtcmPackageIds;
END;
$$;

CREATE OR REPLACE FUNCTION public.insert_galileo_observations(decodedObsFrame json)
 RETURNS VOID
 LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO galileo_observations
    (rtcm_package_id, mountpoint, obs_epoch, rtcm_msg_type, sat_id, sat_signal, obs_code, obs_phase, obs_doppler, obs_snr, obs_lock_time_indicator)
    SELECT (json_array_elements->>0)::bigint, 
           (json_array_elements->>1)::text, 
           (json_array_elements->>2)::timestamp with time zone, 
           (json_array_elements->>3)::smallint, 
           (json_array_elements->>4)::char(4), 
           (json_array_elements->>5)::char(3), 
           (json_array_elements->>6)::numeric(13, 10), 
           (json_array_elements->>7)::numeric(14, 11), 
           (json_array_elements->>8)::numeric(8, 4), 
           (json_array_elements->>9)::numeric(6, 4), 
           (json_array_elements->>10)::integer
    FROM json_array_elements(decodedObsFrame);
END;
$$;

CREATE OR REPLACE FUNCTION public.insert_gps_observations(decodedObsFrame json)
 RETURNS VOID
 LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO gps_observations
    (rtcm_package_id, mountpoint, obs_epoch, rtcm_msg_type, sat_id, sat_signal, obs_code, obs_phase, obs_doppler, obs_snr, obs_lock_time_indicator)
    SELECT (json_array_elements->>0)::bigint, 
           (json_array_elements->>1)::text, 
           (json_array_elements->>2)::timestamp with time zone, 
           (json_array_elements->>3)::smallint, 
           (json_array_elements->>4)::char(4), 
           (json_array_elements->>5)::char(3), 
           (json_array_elements->>6)::numeric(13, 10), 
           (json_array_elements->>7)::numeric(14, 11), 
           (json_array_elements->>8)::numeric(8, 4), 
           (json_array_elements->>9)::numeric(6, 4), 
           (json_array_elements->>10)::integer
    FROM json_array_elements(decodedObsFrame);
END;
$$;

CREATE OR REPLACE FUNCTION public.insert_glonass_observations(decodedObsFrame json)
 RETURNS VOID
 LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO glonass_observations
    (rtcm_package_id, mountpoint, obs_epoch, rtcm_msg_type, sat_id, sat_signal, obs_code, obs_phase, obs_doppler, obs_snr, obs_lock_time_indicator)
    SELECT (json_array_elements->>0)::bigint, 
           (json_array_elements->>1)::text, 
           (json_array_elements->>2)::timestamp with time zone, 
           (json_array_elements->>3)::smallint, 
           (json_array_elements->>4)::char(4), 
           (json_array_elements->>5)::char(3), 
           (json_array_elements->>6)::numeric(13, 10), 
           (json_array_elements->>7)::numeric(14, 11), 
           (json_array_elements->>8)::numeric(8, 4), 
           (json_array_elements->>9)::numeric(6, 4), 
           (json_array_elements->>10)::integer
    FROM json_array_elements(decodedObsFrame);
END;
$$;

CREATE OR REPLACE FUNCTION public.insert_beidou_observations(decodedObsFrame json)
 RETURNS VOID
 LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO beidou_observations
    (rtcm_package_id, mountpoint, obs_epoch, rtcm_msg_type, sat_id, sat_signal, obs_code, obs_phase, obs_doppler, obs_snr, obs_lock_time_indicator)
    SELECT (json_array_elements->>0)::bigint, 
           (json_array_elements->>1)::text, 
           (json_array_elements->>2)::timestamp with time zone, 
           (json_array_elements->>3)::smallint, 
           (json_array_elements->>4)::char(4), 
           (json_array_elements->>5)::char(3), 
           (json_array_elements->>6)::numeric(13, 10), 
           (json_array_elements->>7)::numeric(14, 11), 
           (json_array_elements->>8)::numeric(8, 4), 
           (json_array_elements->>9)::numeric(6, 4), 
           (json_array_elements->>10)::integer
    FROM json_array_elements(decodedObsFrame);
END;
$$;

CREATE OR REPLACE FUNCTION public.insert_qzss_observations(decodedObsFrame json)
 RETURNS VOID
 LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO qzss_observations
    (rtcm_package_id, mountpoint, obs_epoch, rtcm_msg_type, sat_id, sat_signal, obs_code, obs_phase, obs_doppler, obs_snr, obs_lock_time_indicator)
    SELECT (json_array_elements->>0)::bigint, 
           (json_array_elements->>1)::text, 
           (json_array_elements->>2)::timestamp with time zone, 
           (json_array_elements->>3)::smallint, 
           (json_array_elements->>4)::char(4), 
           (json_array_elements->>5)::char(3), 
           (json_array_elements->>6)::numeric(13, 10), 
           (json_array_elements->>7)::numeric(14, 11), 
           (json_array_elements->>8)::numeric(8, 4), 
           (json_array_elements->>9)::numeric(6, 4), 
           (json_array_elements->>10)::integer
    FROM json_array_elements(decodedObsFrame);
END;
$$;

CREATE OR REPLACE FUNCTION public.insert_sbas_observations(decodedObsFrame json)
 RETURNS VOID
 LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO sbas_observations
    (rtcm_package_id, mountpoint, obs_epoch, rtcm_msg_type, sat_id, sat_signal, obs_code, obs_phase, obs_doppler, obs_snr, obs_lock_time_indicator)
    SELECT (json_array_elements->>0)::bigint, 
           (json_array_elements->>1)::text, 
           (json_array_elements->>2)::timestamp with time zone, 
           (json_array_elements->>3)::smallint, 
           (json_array_elements->>4)::char(4), 
           (json_array_elements->>5)::char(3), 
           (json_array_elements->>6)::numeric(13, 10), 
           (json_array_elements->>7)::numeric(14, 11), 
           (json_array_elements->>8)::numeric(8, 4), 
           (json_array_elements->>9)::numeric(6, 4), 
           (json_array_elements->>10)::integer
    FROM json_array_elements(decodedObsFrame);
END;
$$;

CREATE OR REPLACE FUNCTION public.insert_sourcetable_constants(mountpointTable json)
 RETURNS VOID
 LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO sourcetable_constants
    (mountpoint, casterprovider, city, rtcmver, countrycode, latitude, longitude, receiver)
    SELECT (json_array_elements->>0)::text, 
           (json_array_elements->>1)::text, 
           (json_array_elements->>2)::text, 
           (json_array_elements->>3)::text, 
           (json_array_elements->>4)::text, 
           (json_array_elements->>5)::decimal(7,4), 
           (json_array_elements->>6)::decimal(7,4), 
           (json_array_elements->>7)::text
    FROM json_array_elements(mountpointTable)
    ON CONFLICT (mountpoint, countrycode, casterprovider)
    DO UPDATE SET 
        city = EXCLUDED.city, 
        rtcmver = EXCLUDED.rtcmver, 
        latitude = EXCLUDED.latitude, 
        longitude = EXCLUDED.longitude, 
        receiver = EXCLUDED.receiver;
END;
$$;

CREATE OR REPLACE FUNCTION public.insert_disconnect_log(logData json, OUT disconnectlogid int)
RETURNS int
LANGUAGE plpgsql
AS $$
BEGIN
    -- Insert into connection_logger and get the ID
    INSERT INTO connection_logger (mountpoint, disconnect_time)
    VALUES (
        (logData->>0)::text, 
        (logData->>1)::timestamp without time zone
    )
    RETURNING id INTO disconnectlogid;
END;
$$;

CREATE OR REPLACE FUNCTION public.update_reconnect_log(logData json)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    -- Update reconnect_time in connection_logger
    UPDATE connection_logger
    SET reconnect_time = (logData->>1)::timestamp without time zone
    WHERE id = (logData->>0)::int;
    RETURN;
END;
$$;

CREATE OR REPLACE FUNCTION public.upsert_coordinates(decodedObsFrame json)
 RETURNS VOID
 LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO coordinates
    (rtcm_package_id, rtcm_msg_type, mountpoint, ecef_x, ecef_y, ecef_z, antHgt)
    SELECT (json_array_elements->>0)::bigint, 
           (json_array_elements->>1)::smallint, 
           (json_array_elements->>2)::text, 
           (json_array_elements->>3)::numeric(10, 3),
           (json_array_elements->>4)::numeric(10, 3),
           (json_array_elements->>5)::numeric(10, 3),
           (json_array_elements->>6)::numeric(10, 3)
    FROM json_array_elements(decodedObsFrame)
    ON CONFLICT (mountpoint) DO UPDATE SET
        rtcm_package_id = EXCLUDED.rtcm_package_id,
        rtcm_msg_type = EXCLUDED.rtcm_msg_type,
        ecef_x = EXCLUDED.ecef_x,
        ecef_y = EXCLUDED.ecef_y,
        ecef_z = EXCLUDED.ecef_z,
        antHgt = EXCLUDED.antHgt;
END;
$$;
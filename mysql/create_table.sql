CREATE DATABASE IF NOT EXISTS aqi_monitoring;

USE aqi_monitoring;

DROP TABLE IF EXISTS aqi_data;

CREATE TABLE aqi_data (

    last_updated_datetime DATETIME NOT NULL,

    station_name VARCHAR(100) NOT NULL,

    latitude DECIMAL(10,7) NOT NULL,
    longitude DECIMAL(10,7) NOT NULL,

    pm2 DECIMAL(6,2),
    pm10 DECIMAL(6,2),

    no2 DECIMAL(6,2),
    so2 DECIMAL(6,2),

    co DECIMAL(8,2),

    temperature DECIMAL(5,2),

    humidity DECIMAL(5,2),

    air_pressure DECIMAL(7,2),

    air_index INT
);

DROP TABLE IF EXISTS raw_aqi_data;

CREATE TABLE raw_aqi_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    NAME VARCHAR(100) NOT NULL,

    HUMIDITY DECIMAL(5,2),
    LIGHT DECIMAL(10,2),

    NO_MAX DECIMAL(8,2),
    NO_MIN DECIMAL(8,2),

    NO2_MAX DECIMAL(8,2),
    NO2_MIN DECIMAL(8,2),

    OZONE_MAX DECIMAL(8,2),
    OZONE_MIN DECIMAL(8,2),

    PM10_MAX DECIMAL(8,2),
    PM10_MIN DECIMAL(8,2),

    PM2_MAX DECIMAL(8,2),
    PM2_MIN DECIMAL(8,2),

    SO2_MAX DECIMAL(8,2),
    SO2_MIN DECIMAL(8,2),

    CO_MAX DECIMAL(8,2),
    CO_MIN DECIMAL(8,2),

    CO2_MAX DECIMAL(10,2),
    CO2_MIN DECIMAL(10,2),

    SOUND DECIMAL(8,2),

    TEMPRATURE_MAX DECIMAL(5,2),
    TEMPRATURE_MIN DECIMAL(5,2),

    UV_MAX DECIMAL(8,2),
    UV_MIN DECIMAL(8,2),

    AIR_PRESSURE DECIMAL(8,2),

    LASTUPDATEDATETIME DATETIME,

    Lattitude DECIMAL(10,7),
    Longitude DECIMAL(10,7)
);
DROP TABLE IF EXISTS producer_checkpoint;
CREATE TABLE producer_checkpoint (
    id INT NOT NULL,
    last_row INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
);

INSERT INTO producer_checkpoint (id, last_row)
VALUES (1, 0);

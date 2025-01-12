DROP TABLE IF EXISTS weather;

CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    airport_icao VARCHAR(10) NOT NULL,
    timestamp VARCHAR(10) NOT NULL,
    wind VARCHAR(10),
    visibility VARCHAR(10),
    sky_condition VARCHAR(20),
    temperature VARCHAR(20),
    altimeter DECIMAL
);

-- DROP TABLE weather;

-- CREATE TABLE weather(


CREATE TABLE IF NOT EXISTS weather (    -- KMCO 042353Z 04004KT 10SM CLR 14/M02 A3029 
    id SERIAL PRIMARY KEY,
    airport_icao VARCHAR(10) NOT NULL,  -- KMCO
    timestamp VARCHAR(10) NOT NULL,     -- 042353Z
    wind VARCHAR(10)                   -- 04004KT
    -- visibility VARCHAR(10),             -- 10SM
    -- sky_condition VARCHAR(10),          -- CLR
    -- temperature VARCHAR(10),            -- 14/M02
    -- altimeter DECIMAL                   -- A3029
);
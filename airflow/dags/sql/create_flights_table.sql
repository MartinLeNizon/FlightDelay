DROP TABLE IF EXISTS flights;

CREATE TABLE IF NOT EXISTS flights (
    id SERIAL PRIMARY KEY,
    scheduled_time TIMESTAMP NOT NULL,
    departure_icao VARCHAR(10) NOT NULL,
    arrival_icao VARCHAR(10) NOT NULL,
    delay_in_minutes INT,
    flight_status VARCHAR(50),
    airline VARCHAR(100),
    flight_number VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS flights (
    id VARCHAR PRIMARY KEY,
    scheduled_time TIMESTAMP NOT NULL,
    departure_iata VARCHAR(3) NOT NULL,
    arrival_iata VARCHAR(3) NOT NULL,
    delay_in_minutes INT,
    flight_status VARCHAR(50)
);
-- Enable the UUID extension if you want to store GUFIs as native UUIDs
-- (Note: FAA GUFIs are often strings, so VARCHAR(100) is safer for strict SWIM)
CREATE TABLE flights (
    -- Identification
    gufi VARCHAR(100) PRIMARY KEY,
    callsign VARCHAR(12),
    tail_number VARCHAR(20),
    
    -- Carrier Logic
    operator VARCHAR(10),  -- The 'Actual' flying airline (e.g., SKW)
    major VARCHAR(10),     -- The 'Marketing' brand (e.g., DAL)
    
    -- Flight Details
    aircraft_type VARCHAR(10),
    origin VARCHAR(4),
    destination VARCHAR(4),
    
    -- The "Time Matrix"
    original_etd TIMESTAMP, -- Set once on first message
    updated_etd TIMESTAMP,  -- Updated with every new message
    original_eta TIMESTAMP, -- Set once on first message
    updated_eta TIMESTAMP,  -- Updated with every new message
    flight_status VARCHAR(20),     -- Flight status (e.g., PLANNED, ACTIVE, or CANCELLED.)
    
    -- Internal Tracking
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for your FastAPI endpoints
CREATE INDEX idx_flights_callsign ON flights(callsign);
CREATE INDEX idx_flights_major ON flights(major);
CREATE INDEX idx_flights_destination ON flights(destination);
CREATE INDEX idx_flights_last_updated ON flights(last_updated);
CREATE INDEX idx_flights_status ON flights(flight_status);
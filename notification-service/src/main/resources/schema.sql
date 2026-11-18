CREATE TABLE IF NOT EXISTS processed_event (
    event_key  VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT now()
);

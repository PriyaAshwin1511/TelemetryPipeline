-- Create telemetry table
CREATE TABLE IF NOT EXISTS telemetry (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    metric_name VARCHAR NOT NULL,
    gpu_id VARCHAR,
    device VARCHAR,
    uuid VARCHAR NOT NULL,
    model_name VARCHAR,
    hostname VARCHAR,
    container VARCHAR,
    pod VARCHAR,
    namespace VARCHAR,
    value NUMERIC NOT NULL,
    labels JSONB
);

-- Create index on UUID and timestamp for efficient time-range queries by UUID
CREATE INDEX IF NOT EXISTS idx_uuid_timestamp ON telemetry(uuid, timestamp);

-- Add comment to table
COMMENT ON TABLE telemetry IS 'Telemetry data from GPU metrics';

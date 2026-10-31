-- Additive schema only; existing internal tables and views are untouched.
CREATE TABLE IF NOT EXISTS api_products (
 id BIGINT PRIMARY KEY, title TEXT, description TEXT, category TEXT, price NUMERIC(14,2),
 discount_percentage NUMERIC(8,4), rating NUMERIC(5,2), stock INTEGER, brand TEXT, sku TEXT,
 thumbnail TEXT, raw_data JSONB NOT NULL, source_system TEXT NOT NULL DEFAULT 'dummyjson',
 ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS idx_api_products_category ON api_products(category);
CREATE INDEX IF NOT EXISTS idx_api_products_sku ON api_products(sku);
CREATE INDEX IF NOT EXISTS idx_api_products_raw_data ON api_products USING GIN(raw_data);
CREATE TABLE IF NOT EXISTS api_users (
 id BIGINT PRIMARY KEY, username TEXT, email TEXT, first_name TEXT, last_name TEXT, gender TEXT,
 age INTEGER, birth_date DATE, phone TEXT, country TEXT, city TEXT, state TEXT, raw_data JSONB NOT NULL,
 source_system TEXT NOT NULL DEFAULT 'dummyjson', ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS idx_api_users_country ON api_users(country);
CREATE INDEX IF NOT EXISTS idx_api_users_raw_data ON api_users USING GIN(raw_data);
CREATE TABLE IF NOT EXISTS api_carts (
 id BIGINT PRIMARY KEY, user_id BIGINT, total NUMERIC(14,2), discounted_total NUMERIC(14,2),
 total_products INTEGER, total_quantity INTEGER, products JSONB NOT NULL DEFAULT '[]'::jsonb,
 raw_data JSONB NOT NULL, source_system TEXT NOT NULL DEFAULT 'dummyjson', ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS idx_api_carts_user_id ON api_carts(user_id);
CREATE INDEX IF NOT EXISTS idx_api_carts_products ON api_carts USING GIN(products);
CREATE INDEX IF NOT EXISTS idx_api_carts_raw_data ON api_carts USING GIN(raw_data);
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_api_carts_user') THEN
  ALTER TABLE api_carts ADD CONSTRAINT fk_api_carts_user FOREIGN KEY (user_id) REFERENCES api_users(id);
 END IF;
END $$;
CREATE TABLE IF NOT EXISTS api_ingestion_runs (
 run_id BIGSERIAL PRIMARY KEY, entity TEXT NOT NULL, started_at TIMESTAMPTZ NOT NULL,
 completed_at TIMESTAMPTZ, fetched_count INTEGER NOT NULL DEFAULT 0, loaded_count INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL, error_message TEXT);
CREATE INDEX IF NOT EXISTS idx_api_ingestion_runs_started_at ON api_ingestion_runs(started_at);
CREATE TABLE IF NOT EXISTS simulated_events (
 event_id UUID PRIMARY KEY, event_type TEXT NOT NULL, event_timestamp TIMESTAMPTZ NOT NULL,
 customer_id BIGINT, product_id BIGINT, payload JSONB NOT NULL,
 source_system TEXT NOT NULL DEFAULT 'simulated', consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
ALTER TABLE simulated_events ADD COLUMN IF NOT EXISTS processing_latency_ms NUMERIC(14,3);
CREATE INDEX IF NOT EXISTS idx_simulated_events_timestamp ON simulated_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_simulated_events_type ON simulated_events(event_type);
CREATE INDEX IF NOT EXISTS idx_simulated_events_consumed_at ON simulated_events(consumed_at);

CREATE TABLE IF NOT EXISTS streaming_consumer_batches (
 batch_id BIGSERIAL PRIMARY KEY,
 consumer_group TEXT NOT NULL,
 topic TEXT NOT NULL,
 started_at TIMESTAMPTZ NOT NULL,
 completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 received_count INTEGER NOT NULL,
 valid_count INTEGER NOT NULL,
 inserted_count INTEGER NOT NULL,
 duplicate_count INTEGER NOT NULL,
 failed_count INTEGER NOT NULL,
 processing_duration_ms NUMERIC(14,3) NOT NULL,
 status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_streaming_batches_completed_at ON streaming_consumer_batches(completed_at);
CREATE INDEX IF NOT EXISTS idx_streaming_batches_status ON streaming_consumer_batches(status);

CREATE TABLE IF NOT EXISTS streaming_consumer_errors (
 error_id BIGSERIAL PRIMARY KEY,
 consumer_group TEXT NOT NULL,
 topic TEXT NOT NULL,
 partition_id INTEGER,
 offset_number BIGINT,
 event_id TEXT,
 error_type TEXT NOT NULL,
 error_message TEXT NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_streaming_errors_recorded_at ON streaming_consumer_errors(recorded_at);
CREATE INDEX IF NOT EXISTS idx_streaming_errors_type ON streaming_consumer_errors(error_type);

CREATE TABLE IF NOT EXISTS data_quality_runs (
 run_id BIGSERIAL PRIMARY KEY,
 started_at TIMESTAMPTZ NOT NULL,
 completed_at TIMESTAMPTZ,
 status TEXT NOT NULL,
 checks_total INTEGER NOT NULL DEFAULT 0,
 checks_passed INTEGER NOT NULL DEFAULT 0,
 checks_failed INTEGER NOT NULL DEFAULT 0,
 checks_warned INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_data_quality_runs_started_at ON data_quality_runs(started_at);
CREATE TABLE IF NOT EXISTS data_quality_results (
 result_id BIGSERIAL PRIMARY KEY,
 run_id BIGINT NOT NULL REFERENCES data_quality_runs(run_id),
 rule_name TEXT NOT NULL,
 source_table TEXT NOT NULL,
 severity TEXT NOT NULL,
 checked_rows BIGINT NOT NULL,
 failed_rows BIGINT NOT NULL,
 status TEXT NOT NULL,
 details TEXT NOT NULL,
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_data_quality_results_run_id ON data_quality_results(run_id);
CREATE INDEX IF NOT EXISTS idx_data_quality_results_recorded_at ON data_quality_results(recorded_at);

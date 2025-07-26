-- Create main tables
CREATE TABLE IF NOT EXISTS approved (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bdc_cancel_order (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bdc_decline (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS brv_checked (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS depot_manager (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS good_standing (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS loaded (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_released (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ordered (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ppmc_cancel_order (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS depot_manager_decline (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS depot_manager_decline_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create history tables
CREATE TABLE IF NOT EXISTS approved_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bdc_cancel_order_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bdc_decline_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS brv_checked_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS depot_manager_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS good_standing_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS loaded_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_released_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ordered_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ppmc_cancel_order_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create new records table for depot_manager
CREATE TABLE IF NOT EXISTS depot_manager_new_records (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Create new records table for marked
CREATE TABLE IF NOT EXISTS marked (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS marked_history (
    id SERIAL PRIMARY KEY,
    order_date VARCHAR(50),
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume VARCHAR(50),
    ex_ref_price VARCHAR(50),
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_approved_brv_number ON approved(brv_number);
CREATE INDEX IF NOT EXISTS idx_depot_manager_brv_number ON depot_manager(brv_number);
CREATE INDEX IF NOT EXISTS idx_depot_manager_new_records_detected_at ON depot_manager_new_records(detected_at);

-- Add RLS (Row Level Security) policies if needed
-- ALTER TABLE approved ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY "Enable read access for all users" ON approved FOR SELECT USING (true);
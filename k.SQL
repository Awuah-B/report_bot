-- Main tables for each status
CREATE TABLE approved (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bdc_cancel_order (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bdc_decline (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE brv_checked (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE depot_manager (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE good_standing (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE loaded (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_released (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ordered (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ppmc_cancel_order (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE depot_manager_decline (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE marked (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- History tables for each main table
CREATE TABLE approved_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bdc_cancel_order_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bdc_decline_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE brv_checked_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE depot_manager_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE good_standing_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE loaded_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_released_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ordered_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ppmc_cancel_order_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE depot_manager_decline_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE marked_history (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- New records tracking table (specifically for depot_manager)
CREATE TABLE depot_manager_new_records (
    id SERIAL PRIMARY KEY,
    order_date TIMESTAMP,
    order_number VARCHAR(100),
    products VARCHAR(200),
    volume INTEGER,
    ex_ref_price FLOAT,
    brv_number VARCHAR(100),
    bdc VARCHAR(200),
    record_hash VARCHAR(64),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_approved_brv ON approved(brv_number);
CREATE INDEX idx_approved_date ON approved(order_date);
CREATE INDEX idx_approved_hash ON approved(record_hash);

CREATE INDEX idx_bdc_cancel_order_brv ON bdc_cancel_order(brv_number);
CREATE INDEX idx_bdc_cancel_order_date ON bdc_cancel_order(order_date);
CREATE INDEX idx_bdc_cancel_order_hash ON bdc_cancel_order(record_hash);

CREATE INDEX idx_bdc_decline_brv ON bdc_decline(brv_number);
CREATE INDEX idx_bdc_decline_date ON bdc_decline(order_date);
CREATE INDEX idx_bdc_decline_hash ON bdc_decline(record_hash);

CREATE INDEX idx_brv_checked_brv ON brv_checked(brv_number);
CREATE INDEX idx_brv_checked_date ON brv_checked(order_date);
CREATE INDEX idx_brv_checked_hash ON brv_checked(record_hash);

CREATE INDEX idx_depot_manager_brv ON depot_manager(brv_number);
CREATE INDEX idx_depot_manager_date ON depot_manager(order_date);
CREATE INDEX idx_depot_manager_hash ON depot_manager(record_hash);

CREATE INDEX idx_good_standing_brv ON good_standing(brv_number);
CREATE INDEX idx_good_standing_date ON good_standing(order_date);
CREATE INDEX idx_good_standing_hash ON good_standing(record_hash);

CREATE INDEX idx_loaded_brv ON loaded(brv_number);
CREATE INDEX idx_loaded_date ON loaded(order_date);
CREATE INDEX idx_loaded_hash ON loaded(record_hash);

CREATE INDEX idx_order_released_brv ON order_released(brv_number);
CREATE INDEX idx_order_released_date ON order_released(order_date);
CREATE INDEX idx_order_released_hash ON order_released(record_hash);

CREATE INDEX idx_ordered_brv ON ordered(brv_number);
CREATE INDEX idx_ordered_date ON ordered(order_date);
CREATE INDEX idx_ordered_hash ON ordered(record_hash);

CREATE INDEX idx_ppmc_cancel_order_brv ON ppmc_cancel_order(brv_number);
CREATE INDEX idx_ppmc_cancel_order_date ON ppmc_cancel_order(order_date);
CREATE INDEX idx_ppmc_cancel_order_hash ON ppmc_cancel_order(record_hash);

CREATE INDEX idx_depot_manager_decline_brv ON depot_manager_decline(brv_number);
CREATE INDEX idx_depot_manager_decline_date ON depot_manager_decline(order_date);
CREATE INDEX idx_depot_manager_decline_hash ON depot_manager_decline(record_hash);

CREATE INDEX idx_marked_brv ON marked(brv_number);
CREATE INDEX idx_marked_date ON marked(order_date);
CREATE INDEX idx_marked_hash ON marked(record_hash);

-- History table indexes
CREATE INDEX idx_depot_manager_history_hash ON depot_manager_history(record_hash);
CREATE INDEX idx_depot_manager_history_archived ON depot_manager_history(archived_at);

-- New records table indexes
CREATE INDEX idx_depot_manager_new_records_detected ON depot_manager_new_records(detected_at);
CREATE INDEX idx_depot_manager_new_records_brv ON depot_manager_new_records(brv_number);

-- Enable RLS on all tables for security
ALTER TABLE approved ENABLE ROW LEVEL SECURITY;
ALTER TABLE bdc_cancel_order ENABLE ROW LEVEL SECURITY;
ALTER TABLE bdc_decline ENABLE ROW LEVEL SECURITY;
ALTER TABLE brv_checked ENABLE ROW LEVEL SECURITY;
ALTER TABLE depot_manager ENABLE ROW LEVEL SECURITY;
ALTER TABLE good_standing ENABLE ROW LEVEL SECURITY;
ALTER TABLE loaded ENABLE ROW LEVEL SECURITY;
ALTER TABLE order_released ENABLE ROW LEVEL SECURITY;
ALTER TABLE ordered ENABLE ROW LEVEL SECURITY;
ALTER TABLE ppmc_cancel_order ENABLE ROW LEVEL SECURITY;
ALTER TABLE depot_manager_decline ENABLE ROW LEVEL SECURITY;
ALTER TABLE marked ENABLE ROW LEVEL SECURITY;

-- Create policies for service role access (adjust as needed)
CREATE POLICY "Enable all operations for service role" ON approved
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON bdc_cancel_order
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON bdc_decline
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON brv_checked
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON depot_manager
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON good_standing
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON loaded
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON order_released
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON ordered
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON ppmc_cancel_order
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON depot_manager_decline
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON marked
FOR ALL USING (auth.role() = 'service_role');

-- History and new records tables
CREATE POLICY "Enable all operations for service role" ON depot_manager_history
FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "Enable all operations for service role" ON depot_manager_new_records
FOR ALL USING (auth.role() = 'service_role');
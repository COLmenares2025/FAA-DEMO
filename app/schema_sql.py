SCHEMA_SQL = '''
CREATE TABLE IF NOT EXISTS aircraft (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT 'N/A',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS import_batch (
    id INTEGER PRIMARY KEY,
    aircraft_id INTEGER NOT NULL,
    file_name TEXT NOT NULL,
    file_sha256 TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT,
    total_rows INTEGER NOT NULL,
    inserted_rows INTEGER NOT NULL DEFAULT 0,
    error_rows INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (status IN ('uploaded','validated','loaded','failed')),
    FOREIGN KEY (aircraft_id) REFERENCES aircraft(id) ON DELETE CASCADE,
    UNIQUE(file_sha256, aircraft_id)
);

CREATE TABLE IF NOT EXISTS maintenance_item (
    id INTEGER PRIMARY KEY,
    aircraft_id INTEGER NOT NULL,
    import_batch_id INTEGER NOT NULL,
    item_code TEXT,
    position TEXT,
    description TEXT NOT NULL,
    type TEXT,
    interval_months INTEGER CHECK (interval_months IS NULL OR interval_months >= 0),
    interval_hours INTEGER CHECK (interval_hours IS NULL OR interval_hours >= 0),
    interval_landings INTEGER CHECK (interval_landings IS NULL OR interval_landings >= 0),
    adjusted_value INTEGER CHECK (adjusted_value IS NULL OR adjusted_value >= 0),
    adjusted_unit TEXT CHECK (adjusted_unit IS NULL OR adjusted_unit IN ('hrs','ldgs')),
    adjusted_delta INTEGER,
    part_number TEXT,
    part_serial TEXT,
    last_completed_date TEXT,
    last_completed_hours INTEGER CHECK (last_completed_hours IS NULL OR last_completed_hours >= 0),
    last_completed_landings INTEGER CHECK (last_completed_landings IS NULL OR last_completed_landings >= 0),
    last_completed_city TEXT,
    due_next_date TEXT,
    due_next_hours INTEGER CHECK (due_next_hours IS NULL OR due_next_hours >= 0),
    due_next_landings INTEGER CHECK (due_next_landings IS NULL OR due_next_landings >= 0),
    time_remaining_text TEXT,
    months_remaining INTEGER,
    days_remaining INTEGER,
    is_overdue_time INTEGER CHECK (is_overdue_time IN (0,1) OR is_overdue_time IS NULL),
    hours_remaining INTEGER CHECK (hours_remaining IS NULL OR hours_remaining >= 0),
    landings_remaining INTEGER CHECK (landings_remaining IS NULL OR landings_remaining >= 0),
    status TEXT,
    status_note TEXT,
    fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (aircraft_id) REFERENCES aircraft(id) ON DELETE CASCADE,
    FOREIGN KEY (import_batch_id) REFERENCES import_batch(id) ON DELETE CASCADE,
    UNIQUE(import_batch_id, fingerprint)
);

CREATE TABLE IF NOT EXISTS import_error (
    id INTEGER PRIMARY KEY,
    import_batch_id INTEGER NOT NULL,
    row_index INTEGER NOT NULL,
    field TEXT NOT NULL,
    message TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('error','warning')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (import_batch_id) REFERENCES import_batch(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS maintenance_item_quarantine (
    id INTEGER PRIMARY KEY,
    aircraft_id INTEGER NOT NULL,
    import_batch_id INTEGER NOT NULL,
    source_row_index INTEGER NOT NULL,
    reason TEXT NOT NULL,
    error_message TEXT,
    item_code TEXT,
    position TEXT,
    description TEXT,
    type TEXT,
    interval_months INTEGER,
    interval_hours INTEGER,
    interval_landings INTEGER,
    adjusted_value INTEGER,
    adjusted_unit TEXT,
    adjusted_delta INTEGER,
    part_number TEXT,
    part_serial TEXT,
    last_completed_date TEXT,
    last_completed_hours INTEGER,
    last_completed_landings INTEGER,
    last_completed_city TEXT,
    due_next_date TEXT,
    due_next_hours INTEGER,
    due_next_landings INTEGER,
    time_remaining_text TEXT,
    months_remaining INTEGER,
    days_remaining INTEGER,
    is_overdue_time INTEGER,
    hours_remaining INTEGER,
    landings_remaining INTEGER,
    status TEXT,
    status_note TEXT,
    fingerprint TEXT,
    quarantined_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (aircraft_id) REFERENCES aircraft(id) ON DELETE CASCADE,
    FOREIGN KEY (import_batch_id) REFERENCES import_batch(id) ON DELETE CASCADE,
    UNIQUE(import_batch_id, source_row_index)
);

CREATE TABLE IF NOT EXISTS data_ledger (
    id INTEGER PRIMARY KEY,
    ts TEXT NOT NULL DEFAULT (datetime('now')),
    table_name TEXT NOT NULL,
    action TEXT NOT NULL,      -- INSERT / UPDATE
    row_id INTEGER,
    import_batch_id INTEGER,
    actor_user_id INTEGER,
    actor_username TEXT,
    details TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    password_salt TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin','mechanic','auditor')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS user_session (
    token TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_item_aircraft ON maintenance_item(aircraft_id);
CREATE INDEX IF NOT EXISTS idx_item_status ON maintenance_item(status);
CREATE INDEX IF NOT EXISTS idx_item_type ON maintenance_item(type);
CREATE INDEX IF NOT EXISTS idx_item_due_hours ON maintenance_item(due_next_hours);
CREATE INDEX IF NOT EXISTS idx_item_due_landings ON maintenance_item(due_next_landings);
CREATE INDEX IF NOT EXISTS idx_item_due_date ON maintenance_item(due_next_date);
CREATE INDEX IF NOT EXISTS idx_q_batch ON maintenance_item_quarantine(import_batch_id);
CREATE INDEX IF NOT EXISTS idx_q_fp ON maintenance_item_quarantine(fingerprint);
CREATE INDEX IF NOT EXISTS idx_user_session_user ON user_session(user_id);

-- Append-only guards
CREATE TRIGGER IF NOT EXISTS forbid_delete_aircraft
BEFORE DELETE ON aircraft
BEGIN
  SELECT RAISE(ABORT, 'DELETE prohibido: append-only (aircraft)');
END;
CREATE TRIGGER IF NOT EXISTS forbid_delete_import_batch
BEFORE DELETE ON import_batch
BEGIN
  SELECT RAISE(ABORT, 'DELETE prohibido: append-only (import_batch)');
END;
CREATE TRIGGER IF NOT EXISTS forbid_delete_maintenance_item
BEFORE DELETE ON maintenance_item
BEGIN
  SELECT RAISE(ABORT, 'DELETE prohibido: append-only (maintenance_item)');
END;

-- Convenience view: only published items
CREATE VIEW IF NOT EXISTS v_items_loaded AS
SELECT mi.*
FROM maintenance_item mi
JOIN import_batch b ON b.id = mi.import_batch_id
WHERE b.status = 'loaded';
 
-- ===================== MTRs (Maintenance/Technical Report) =====================

-- Estado actual de TIME & CYCLES por aeronave (no histórico)
CREATE TABLE IF NOT EXISTS aircraft_time_cycles (
    aircraft_id INTEGER PRIMARY KEY,
    aircraft_hours REAL CHECK (aircraft_hours IS NULL OR aircraft_hours >= 0),
    aircraft_landings INTEGER CHECK (aircraft_landings IS NULL OR aircraft_landings >= 0),
    apu_hours REAL CHECK (apu_hours IS NULL OR apu_hours >= 0),
    apu_cycles INTEGER CHECK (apu_cycles IS NULL OR apu_cycles >= 0),
    engine_1_hours REAL CHECK (engine_1_hours IS NULL OR engine_1_hours >= 0),
    engine_1_cycles INTEGER CHECK (engine_1_cycles IS NULL OR engine_1_cycles >= 0),
    engine_2_hours REAL CHECK (engine_2_hours IS NULL OR engine_2_hours >= 0),
    engine_2_cycles INTEGER CHECK (engine_2_cycles IS NULL OR engine_2_cycles >= 0),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(aircraft_id) REFERENCES aircraft(id) ON DELETE CASCADE
);

-- Maestro MTR
CREATE TABLE IF NOT EXISTS mtr (
    id INTEGER PRIMARY KEY,
    aircraft_id INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('borrador','enviado')) DEFAULT 'borrador',
    work_complete_date TEXT NOT NULL,
    aircraft_serial_no TEXT NOT NULL,
    aircraft_reg_no TEXT NOT NULL,
    work_complete_city TEXT NOT NULL,
    items_confirmed_at TEXT,
    -- REPAIR FACILITY
    repair_facility TEXT,
    facility_certificate TEXT,
    work_order_number TEXT,
    work_performed_by TEXT,
    performer_certificate_number TEXT,
    repair_date TEXT,
    -- INSPECTION
    additional_certification_statement TEXT,
    work_inspected_by TEXT,
    inspector_certificate_number TEXT,
    inspection_date TEXT,
    created_by INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT,
    submitted_at TEXT,
    FOREIGN KEY (aircraft_id) REFERENCES aircraft(id) ON DELETE CASCADE,
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
);

-- Snapshot de TIME & CYCLES usado por el MTR
CREATE TABLE IF NOT EXISTS mtr_time_cycles_snapshot (
    id INTEGER PRIMARY KEY,
    mtr_id INTEGER NOT NULL UNIQUE,
    aircraft_hours REAL,
    aircraft_landings INTEGER,
    apu_hours REAL,
    apu_cycles INTEGER,
    engine_1_hours REAL,
    engine_1_cycles INTEGER,
    engine_2_hours REAL,
    engine_2_cycles INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (mtr_id) REFERENCES mtr(id) ON DELETE CASCADE
);

-- Ítems asociados al MTR
CREATE TABLE IF NOT EXISTS mtr_item (
    id INTEGER PRIMARY KEY,
    mtr_id INTEGER NOT NULL,
    item_code TEXT NOT NULL,
    description TEXT,
    maintenance_item_id INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (mtr_id) REFERENCES mtr(id) ON DELETE CASCADE,
    FOREIGN KEY (maintenance_item_id) REFERENCES maintenance_item(id) ON DELETE SET NULL,
    UNIQUE(mtr_id, item_code)
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_mtr_aircraft ON mtr(aircraft_id);
CREATE INDEX IF NOT EXISTS idx_mtr_status ON mtr(status);
CREATE INDEX IF NOT EXISTS idx_mtr_item_mtr ON mtr_item(mtr_id);

-- Vista para listado de MTRs
CREATE VIEW IF NOT EXISTS v_mtr_list AS
SELECT 
  m.id,
  m.aircraft_id,
  a.name AS aircraft_name,
  a.model AS aircraft_model,
  m.work_complete_city,
  m.status,
  m.work_complete_date AS date,
  (SELECT COUNT(*) FROM mtr_item mi WHERE mi.mtr_id = m.id) AS numero_de_items
FROM mtr m
JOIN aircraft a ON a.id = m.aircraft_id;
''';

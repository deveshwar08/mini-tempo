package storage

const schemaSQL = `
CREATE TABLE IF NOT EXISTS instances (
    instance_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    sequence_num INTEGER NOT NULL,
    activity_name TEXT NOT NULL,
    payload BLOB,
    completed BOOLEAN NOT NULL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(instance_id, sequence_num),
    FOREIGN KEY(instance_id) REFERENCES instances(instance_id)
);

CREATE INDEX IF NOT EXISTS idx_events_instance ON events(instance_id);
`
package storage

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type SqliteEventStore struct {
	db                 *sql.DB
	stmtInsertInstance *sql.Stmt
	stmtInsertEvent    *sql.Stmt
	stmtGetEvents      *sql.Stmt
	stmtGetStatus      *sql.Stmt
	stmtUpdateStatus   *sql.Stmt
}

func NewSqliteEventStore(dbPath string) (*SqliteEventStore, error) {
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=5000", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	s := &SqliteEventStore{db: db}

	s.stmtInsertInstance, err = db.Prepare(`INSERT INTO instances (instance_id, name, status) VALUES (?, ?, ?)`)
	if err != nil {
		return nil, err
	}
	s.stmtInsertEvent, err = db.Prepare(`INSERT INTO events (instance_id, sequence_num, activity_name, payload, completed) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return nil, err
	}
	s.stmtGetEvents, err = db.Prepare(`SELECT sequence_num, activity_name, payload, completed FROM events WHERE instance_id = ? ORDER BY sequence_num ASC`)
	if err != nil {
		return nil, err
	}
	s.stmtGetStatus, err = db.Prepare(`SELECT status FROM instances WHERE instance_id = ?`)
	if err != nil {
		return nil, err
	}
	s.stmtUpdateStatus, err = db.Prepare(`UPDATE instances SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE instance_id = ?`)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *SqliteEventStore) CreateInstance(ctx context.Context, instanceID string, name string) error {
	_, err := s.stmtInsertInstance.ExecContext(ctx, instanceID, name, "RUNNING")
	if err != nil {
		// Basic check for SQLite unique constraint failure
		if err.Error() == "UNIQUE constraint failed: instances.instance_id" {
			return fmt.Errorf("%w: %s", ErrInstanceExists, instanceID)
		}
		return err
	}
	return nil
}

// Close releases the database resources.
func (s *SqliteEventStore) Close() error {
	s.stmtInsertInstance.Close()
	s.stmtInsertEvent.Close()
	s.stmtGetEvents.Close()
	s.stmtGetStatus.Close()
	s.stmtUpdateStatus.Close()
	return s.db.Close()
}

func (s *SqliteEventStore) AppendEvent(ctx context.Context, instanceID string, event Event) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Stmt(s.stmtInsertEvent).ExecContext(ctx, instanceID, event.SequenceNum, event.ActivityName, event.Payload, event.Completed)
	if err != nil {
		return fmt.Errorf("failed to append event: %w", err)
	}

	return tx.Commit()
}

func (s *SqliteEventStore) GetEvents(ctx context.Context, instanceID string) ([]Event, error) {
	rows, err := s.stmtGetEvents.QueryContext(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.SequenceNum, &event.ActivityName, &event.Payload, &event.Completed); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *SqliteEventStore) GetInstanceStatus(ctx context.Context, instanceID string) (string, error) {
	var status string
	err := s.stmtGetStatus.QueryRowContext(ctx, instanceID).Scan(&status)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	return status, err
}

func (s *SqliteEventStore) UpdateInstanceStatus(ctx context.Context, instanceID string, status string) error {
	res, err := s.stmtUpdateStatus.ExecContext(ctx, status, instanceID)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	return nil
}

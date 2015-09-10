package migrate

import (
	"database/sql"
	"errors"
)

// Postgres is a Database implementation for the PostgreSQL database dialect.
type Postgres struct {
	db   *sql.DB
	lock *sql.Tx
}

// NewPostgres creates a new Postgres database dialect.
func NewPostgres(db *sql.DB) *Postgres {
	return &Postgres{db, nil}
}

// Lock tries to acquire a lock on the schema_migrations_lock table. If it is
// not possible to aqcuire a lock straightaway, an error is returned rather
// than waiting for the lock. This allows the caller to decide whether to retry
// or not.
func (p *Postgres) Lock() error {
	if p.lock != nil {
		return errors.New("lock already acquired")
	}

	_, err := p.db.Exec("CREATE TABLE IF NOT EXISTS schema_migrations_lock(lock boolean PRIMARY KEY);")
	if err != nil {
		return err
	}

	tx, err := p.db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec("LOCK TABLE schema_migrations_lock IN ACCESS EXCLUSIVE MODE NOWAIT")
	if err != nil {
		tx.Rollback()
		return err
	}

	p.lock = tx
	return nil
}

// Unlock releases the lock on the schema_migrations_lock table.
func (p *Postgres) Unlock() error {
	if p.lock == nil {
		return errors.New("unable to unlock, lock not acquired")
	}
	err := p.lock.Rollback()
	p.lock = nil
	return err
}

// Migrations returns a list of migration id's that have been applied to the
// current database.
func (p *Postgres) Migrations() ([]string, error) {
	// create table schema_migrations if it doesn't exist
	_, err := p.db.Exec("CREATE TABLE IF NOT EXISTS schema_migrations(identifier varchar(255) NOT NULL PRIMARY KEY, migrated_at timestamp WITH TIME ZONE NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'));")
	if err != nil {
		return nil, err
	}

	rows, err := p.db.Query("SELECT identifier FROM schema_migrations ORDER BY identifier")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Apply applies migration m to the database.
func (p *Postgres) Apply(m Migration) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec("SET timezone='UTC';"); err != nil {
		return err
	}

	if err = m.Migrate(tx); err != nil {
		return err
	}

	if _, err := tx.Exec("INSERT INTO schema_migrations(identifier) VALUES ($1)", m.ID()); err != nil {
		return err
	}
	return tx.Commit()
}

// Package migrate provides a database agnostic schema migration mechanism with
// a simple database locking strategy. It is suitable to be used in server
// applications where multiple separate processes may attempt to migrate the
// database schema simultaneously.
package migrate

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"
)

var (
	// Logger is a log.Logger used by this package to print info during the
	// migration process. Logging can be disabled by setting this to nil.
	Logger = log.New(os.Stderr, "", log.LstdFlags)

	// LockRetryInterval specifies the duration to wait after an attempt to
	// acquire a lock has failed before trying again.
	LockRetryInterval = 2 * time.Second
)

// Database is an interface that must be implemented for the specific database
// dialect that your application uses.
type Database interface {
	// Lock tries to acquire a database lock. This lock is to prevent multiple
	// separate processes from migrating the database. Not safe to be called
	// concurrently or multiple times from within the same process.
	Lock() error

	// Unlock releases the acquired database lock.
	Unlock() error

	// Migrations returns an ordered list of migration identifiers already
	// applied to the current schema.
	Migrations() ([]string, error)

	// Apply applies the given migration to the current schema.
	Apply(m Migration) error
}

// Migration defines a single migration with a unique identifier ID.
type Migration interface {
	ID() string
	Migrate(e Execer) error
}

// Execer is the SQL exec interface used by the database/sql package.
type Execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// Error represents a failed migration.
type Error struct {
	// ID is the unique identifier of the failed migration.
	ID string
	// Err contains the error that caused the migration to fail.
	Err error
}

func (m Error) Error() string {
	return fmt.Sprintf("migrate failed for %s: %s", m.ID, m.Err.Error())
}

// Do migrates database db with the given migrations. Migrations that haven't
// yet been applied to the current schema will be executed, in order. Every
// time this method is called, the entire list of migrations should be
// provided. New migrations should always be appended to the end, if this is
// not the case an error will be returned.
func Do(db Database, migrations []Migration) error {
	// TODO: should there be some limit here, or a way to cancel it?
	for {
		logf("Trying to acquire migration lock")
		if err := db.Lock(); err != nil {
			logf("Unable to acquire lock: %s", err)
			time.Sleep(LockRetryInterval)
			continue
		}
		logf("Lock acquired")
		break
	}

	defer func() {
		logf("Releasing migration lock")
		if err := db.Unlock(); err != nil {
			logf("unlock error: %s", err)
		}
	}()

	applied, err := db.Migrations()
	if err != nil {
		return err
	}

	if len(applied) > len(migrations) {
		return fmt.Errorf("more migrations have been applied (%d) than there exist (%d)", len(applied), len(migrations))
	}

	for i, m := range applied {
		if m != migrations[i].ID() {
			return fmt.Errorf("unknown migration encountered, expected %s but was %s", m, migrations[i].ID())
		}
	}

	if len(applied) == len(migrations) {
		logf("Database is up to date")
		return nil
	}

	migrations = migrations[len(applied):]
	logf("Applying %d new migrations", len(migrations))
	for _, migration := range migrations {
		logf("applying migration %s\n", migration.ID())
		if err = db.Apply(migration); err != nil {
			return Error{migration.ID(), err}
		}
	}
	return nil
}

func logf(format string, a ...interface{}) {
	if Logger != nil {
		Logger.Printf("[migrate] %s", fmt.Sprintf(format, a...))
	}
}

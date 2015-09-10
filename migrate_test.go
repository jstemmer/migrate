package migrate

import (
	"database/sql"
	"testing"
)

type TestMigration struct {
	id       string
	migrated bool
}

func (t TestMigration) ID() string {
	return t.id
}

func (t *TestMigration) Migrate(e Execer) error {
	t.migrated = true
	return nil
}

type TestResult struct{}

func (TestResult) LastInsertId() (int64, error) { return 0, nil }
func (TestResult) RowsAffected() (int64, error) { return 0, nil }

type TestDatabase struct {
	locked     bool
	unlocked   bool
	migrations []string
	count      int
}

func (t *TestDatabase) Lock() error {
	t.locked = true
	return nil
}

func (t *TestDatabase) Unlock() error {
	t.unlocked = true
	return nil
}

func (t *TestDatabase) Migrations() ([]string, error) {
	return t.migrations, nil
}

func (t *TestDatabase) Apply(m Migration) error {
	t.count++
	m.Migrate(t)
	t.migrations = append(t.migrations, m.ID())
	return nil
}

func (t *TestDatabase) Exec(query string, a ...interface{}) (sql.Result, error) {
	return &TestResult{}, nil
}

func TestDo(t *testing.T) {
	Logger = nil

	db := &TestDatabase{}
	migrations := []Migration{
		&TestMigration{id: "migration 1"},
		&TestMigration{id: "migration 2"},
	}

	err := Do(db, migrations)
	if err != nil {
		t.Fatalf("migrate.Do failed: %s", err)
	}

	if db.count != 2 {
		t.Errorf("Incorrect number of migrations applied. Got %d, want %d", db.count, 2)
	}
}

func TestLocking(t *testing.T) {
	t.Skip("pending")
}

func TestFailures(t *testing.T) {
	t.Skip("pending")
}

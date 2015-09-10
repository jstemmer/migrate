package migrate

import (
	"testing"
)

func TestFileMigrations(t *testing.T) {
	expectedMigrations := []string{"file1.sql", "file2.sql"}

	migrations, err := FileMigrations("./fixtures")
	if err != nil {
		t.Fatal(err)
	}

	if len(migrations) != len(expectedMigrations) {
		t.Fatalf("FileMigrations returned incorrect number of migrations. Got %d, want %d", len(migrations), len(expectedMigrations))
	}

	for i, m := range migrations {
		if m.ID() != expectedMigrations[i] {
			t.Errorf("%d: Incorrect migration name. Got %s, want %s", i, m.ID(), expectedMigrations[i])
		}
	}
}

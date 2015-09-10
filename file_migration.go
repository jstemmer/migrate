package migrate

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"strings"
)

// FileMigration is a file containing SQL statements.
type FileMigration string

// ID returns only the filename without the directory.
func (f FileMigration) ID() string {
	_, name := filepath.Split(string(f))
	return name
}

// Migrate takes the contents of the FileMigration file and executes it.
func (f FileMigration) Migrate(e Execer) error {
	data, err := ioutil.ReadFile(string(f))
	if err != nil {
		return err
	}
	_, err = e.Exec(string(data))
	return err
}

// FileMigrations returns a sorted list of all the .sql files in dir.
func FileMigrations(dir string) ([]Migration, error) {
	files, err := filepath.Glob(fmt.Sprintf("%s/*.sql", dir))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	var migrations []Migration
	for _, file := range files {
		if _, name := filepath.Split(file); strings.HasPrefix(name, ".") {
			continue
		}
		migrations = append(migrations, FileMigration(file))
	}
	return migrations, nil
}

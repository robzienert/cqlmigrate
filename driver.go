package cqlmigrate

import (
	"fmt"
	"log"
	"strings"

	"github.com/gocql/gocql"
)

const notFoundError = "not found"

type driver struct {
	keyspace string
	session  *gocql.Session
}

func (d *driver) setupMigrations() error {
	ok, err := d.migrationsTableExists()
	if err != nil {
		return fmt.Errorf("could not ensure migrations table exists: %s", err.Error())
	}
	if !ok {
		err := d.session.Query("CREATE TABLE IF NOT EXISTS migrations (name text, sha text, PRIMARY KEY (name));").Exec()
		if err != nil {
			return fmt.Errorf("could not create missing migrations table: %s", err.Error())
		}
	}
	return nil
}

func (d *driver) migrationsTableExists() (bool, error) {
	var name string
	err := d.session.Query(`SELECT columnfamily_name FROM System.schema_columnfamilies
    WHERE keyspace_name=? and columnfamily_name = 'migrations'`, d.keyspace).Scan(&name)
	if err != nil && err.Error() != notFoundError {
		return false, fmt.Errorf("failed performing CQL query: %s", err.Error())
	}
	return name != "", nil
}

func (d *driver) runMigration(spec Spec, sha string, override bool) error {
	if !d.markMigration(spec, sha, override) {
		// Not running cassandra migration as another process has already marked it
		return nil
	}

	var runStatements []string
	for _, statement := range strings.Split(spec.Data, ";") {
		trimmedStatement := strings.TrimSpace(statement)
		if trimmedStatement != "" {
			log.Println(trimmedStatement)
			err := d.session.Query(trimmedStatement + ";").Exec()
			if err != nil {
				log.Printf("bad query:\n%s", trimmedStatement)
				if len(runStatements) > 0 {
					log.Printf("statements run prior to failure:\n%s\n", strings.Join(runStatements, ";\n"))
				}
				log.Printf("removing mark for migration %s", spec.Name)
				d.removeMigration(spec)
				return fmt.Errorf("bad query: %s", err.Error())
			}
			runStatements = append(runStatements, trimmedStatement)
		}
	}
	return nil
}

func (d *driver) markMigration(spec Spec, sha string, override bool) bool {
	var ifClause string
	if override {
		ifClause = "IF NOT EXISTS"
	}

	err := d.session.Query("INSERT INTO migrations (name, sha) VALUES (?, ?) "+ifClause+";", spec.Name, sha).Exec()
	if err != nil {
		// TODO handle conn err condition; rather than assuming something else is
		// running the migration.
		log.Printf("could not mark migration: %s, err: %s\n", spec.Name, err.Error())
		return false
	}
	return true
}

func (d *driver) removeMigration(spec Spec) {
	err := d.session.Query("DELETE FROM migrations WHERE name = ?", spec.Name).Exec()
	if err != nil {
		log.Printf("could not remove migration %s, err: %s\n", spec.Name, err.Error())
	}
}

func (d *driver) getMigrationMD5(name string) (string, error) {
	var sha string
	err := d.session.Query("SELECT sha FROM migrations WHERE name=?", name).Scan(&sha)
	if err != nil && err.Error() != notFoundError {
		return "", err
	}
	return sha, nil
}

package cqlmigrate

import (
	"fmt"

	"github.com/gocql/gocql"
)

// Spec defines a single migration step.
//
// Name must be unique, and should be human readable and meaningful.
//
// Data is the CQL schema definition for the migration.
type Spec struct {
	Name string
	Data string
}

// Config defines the configurations for performing a migration.
//
// Session is the gocql session to run the migrations with.
//
// Keyspace is which Cassandra keyspace the migrations will be run against.
//
// Override can be set to override any previous migrations that may not match the
// original checksum. By default, this is disabled - use this flag with caution!
type Config struct {
	Session  *gocql.Session
	Keyspace string
	Override bool
}

// Runner performs the CQL migrations.
type Runner struct {
	driver   *driver
	override bool
}

// New creates a new migration Runner
func New(conf *Config) *Runner {
	driver := &driver{keyspace: conf.Keyspace, session: conf.Session}
	return &Runner{driver: driver, override: conf.Override}
}

// Run performs all of the given migrations. The return value of this function
// consists of whether or migrations were successfully run, and the second if
// any errors were encountered.
func (r *Runner) Run(migrations []Spec) (bool, error) {
	if len(migrations) == 0 {
		return false, nil
	}
	if err := r.driver.setupMigrations(); err != nil {
		return false, fmt.Errorf("could not setup migrations: %s", err.Error())
	}

	handler := newDefaultHandler(r.driver, r.override)
	for _, m := range migrations {
		if err := handler.handle(m.Name, m.Data); err != nil {
			return false, fmt.Errorf("failed handling migration: %s", err.Error())
		}
	}
	return true, nil
}

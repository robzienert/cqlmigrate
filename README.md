# cqlmigrate

A Cassandra CQL schema migration library for Golang. With this library, you can
auto-perform updates to your Cassandra keyspaces without a separate deployment
step.

```
package main

import (
	"github.com/gocql/gocql"
  "github.com/robzienert/cqlmigrate"
)

var migrations = []migration.Spec{
  {
    Name: "2016-01-01-initial_release",
    Data: `
    CREATE TABLE foo (
      my_key varchar,
      my_field varchar
      PRIMARY KEY(my_key)
    );
    `,
  },
}

func main() {
  session, _ := gocql.NewCluster("127.0.0.1").CreateSession()
  
  conf := cqlmigrate.Config{
    Session: session,
    Keyspace: "my_keyspace",
  }
  
  if err := cqlmigrate.New(conf).Run(migrations); err != nil {
    panic(err)
  }
}
```

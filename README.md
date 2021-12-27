# godatafarm

A golang library to aid random data generation, mainly for performance testing.

This tool provides a library of standard and repetitive tasks when setting up test data. Skipping some infrastructure and SQL setup, we can focus on logic that randomizes/generates the data.

## A Background

When an app is still early in the development stage, we cannot test performance or database index because there is no data. At times, we need to weigh different solutions with varying performance/ app latency implications; developers end up writing scripts to generate big randomized data, observe the response latency, or analyze the query planner. There are some repetitive tasks in that activity, which this library aims to aid, so we can focus on logic that randomizes/generates the data.

Currently, it only supports Postgres but will cover other RDBMS and non-RDBMS storage

## Installation

```bash
go get github.com/kerocen/godatafarm
```

## Usage

Following example connect to a local Postgres and generate some mock data using [gofakeit](https://github.com/brianvoe/gofakeit). While the struct definition might look like an ORM, it is not. It is just to simplify table representations. This library generates a native prepared statement directly to the database driver. 

```go
package main

import (
	"context"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	"godatafarm/farm"
	"godatafarm/rdbms"
	"time"
)

func main() {
	// define the table sceham using rdbms.Table
	userTable := rdbms.Table{
		Name: "user",
		Columns: []rdbms.Column{
			{
				Name: "user_id",
			},
			{
				Name: "name",
			},
			{
				Name: "phone",
			},
			{
				Name: "email",
			},
			{
				Name: "created_at",
			},
		},
	}
	userSyncTable := rdbms.Table{
		Name: "user_sync",
		Columns: []rdbms.Column{
			{
				Name: "user_id",
			},
			{
				Name: "sync_at",
			},
		},
		ParentTables: []string{"user"},
	}
	userTable.Seeder, userSyncTable.Seeder = createTableSeeder(20)
	tables := []rdbms.Table{userTable, userSyncTable}

	// instantiate the dataFarm
	dataFarm := farm.NewFarm()
	// PostgresConnectionBasic is only a helper tool to create postgres DSN
	pgConnString := farm.PostgresConnectionBasic("root", "root", "localhost", 5432, "postgres")
	// attach Postgres to the dataFarm
	err := dataFarm.PostgresFromConnectionString(pgConnString, tables)
	if err != nil {
		fmt.Printf("error farm pg: %v", err)
	}
	err = dataFarm.SeedPostgres(context.Background())
	if err != nil {
		fmt.Printf("error seeding pg: %v", err)
	}
}

func createTableSeeder(count int) (user *rdbms.ArrayBasedColumnSeeder, userSync *rdbms.ArrayBasedColumnSeeder) {
	user = &rdbms.ArrayBasedColumnSeeder{
		Rows: make([][]interface{}, count),
	}
	userSync = &rdbms.ArrayBasedColumnSeeder{
		Rows: make([][]interface{}, count),
	}
	for i := 0; i < count; i++ {
		userID := uuid.NewString()
		user.Rows[i] = []interface{}{
			userID,
			gofakeit.Name(),
			gofakeit.Phone(),
			gofakeit.Email(),
			time.Now(),
		}
		userSync.Rows[i] = []interface{}{
			userID,
			time.Now(),
		}
	}
	return user, userSync
}

```

## Contributing and Future Improvement

I build this tool for personal usage. I might prioritise the features based on my needs. But, pull requests, feature requests, or feedbacks are welcome
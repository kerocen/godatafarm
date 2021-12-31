package rdbms

import (
	"time"
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	"github.com/rs/zerolog"
	dataFarmErr "godatafarm/error"
	"strings"
)

// ColumnSeeder implementation that uses 2D array
// TODO currently we only have an array, later if the data is too large, can be improved to a channel
type ArrayBasedColumnSeeder struct {
	Rows          [][]interface{}
	currentRowPos int
}

// ColumnSeeder seeds data when BulkInsert is ongoing
type ColumnSeeder interface {
	// Next fetch the next data
	// returns row a slice that contains the data
	// hasData tell if the current position has data. It should be used as a signal that there is no more data
	// if hasData == false or hasData == true, seeder must not return any error
	// e.g.
	/*
		for {
			row, hasData, err := seeder.Next()
			if err != nil {
				return err // or wrap and close connection as needed
			}
			if !hasData {
				break
			}
			BulkInsert(row)
		}
	*/
	Next() (row []interface{}, hasData bool, err error)

	// return how many data has been seeded
	LatestDataCount() int
}

func (a *ArrayBasedColumnSeeder) Next() (row []interface{}, hasData bool, err error) {
	if a.currentRowPos < 0 {
		return nil, false, fmt.Errorf("currentRowPos < 0")
	}
	lastIdx := len(a.Rows) - 1
	if len(a.Rows) == 0 || a.currentRowPos > lastIdx {
		return nil, false, nil
	}
	row = a.Rows[a.currentRowPos]
	a.currentRowPos++
	return row, true, nil
}

func (a *ArrayBasedColumnSeeder) LatestDataCount() int {
	return a.currentRowPos - 1
}

// Column repesent a column/field in the table. Name should match the database column name
type Column struct {
	Name string
}

// Table represent a table.
// Name should match the table name
//
// It is not necessary to define all Columns because the generated prepared statement will be a follwing:
// INSERT INTO table_name (col1, col2) VALUES (x, y)
// so it respects only column that is supplied
//
// Seeder is any implementation of ColumnSeeder
//
// ParentTables is to define all parent table if the table has any foreign key (parent-child) relationships
// it is not mandatory, but if specified, it helps BulkInsert understands in which order the tables should be inserted
// if not, the bulk insert follows the order of table in Instance creation
type Table struct {
	Name         string
	Columns      []Column
	Seeder       ColumnSeeder
	ParentTables []string
}

// Instance is an instance of RDBMS.
type Instance interface {
	// InsertBulk inserts all table by the order of table specified in creation or by following the parent table
	InsertBulk(ctx context.Context, count int) error
	// InsertBulk table inserts only 1 table. But the table should be registered on creation.
	// If not it raises an error because the prepared statement for that table is not built
	InsertBulkTable(ctx context.Context, table Table) error
}

type Postgres struct {
	db             *sql.DB
	tables         []Table
	log            zerolog.Logger
	bulkInsertStmt map[string]*sql.Stmt
}

/*
error code should only be returned by a exported function, smallest call depth as possible
possible error code schema
[a-z]+-[a-z]+-[a-z]+
[a-z]+ = 1st part:  where does the error first found
[a-z] = 2nd part: increasing sequence
	a. error can be fixed by changing input
	b. error is caused by system/infra (e.g. database bad connection)
	...
[a-z] = 3rd part: increasing sequence, specific identifier for the error, unique to the first two part
*/
const (
	invalidTableSchemeError      = "rdbms-a-a"
	invalidConnectionStringError = "rdbms-a-b"
	failedToInitialiseDB         = "rdbms-a-c"

	failedToConnectDBError = "rdbms-b-a"
)

func (p *Postgres) validateTables() error {
	names := make(map[string]interface{})
	for _, t := range p.tables {
		if _, ok := names[t.Name]; ok {
			return fmt.Errorf("duplicate table with name: %s", t.Name)
		}
		names[t.Name] = struct{}{}
	}

	for _, t := range p.tables {
		if len(t.ParentTables) > 0 {
			for _, p := range t.ParentTables {
				if _, ok := names[p]; !ok {
					return fmt.Errorf("parent table %s does not exists", p)
				}
			}
		}
	}
	return nil
}

// TODO combine commits from multiple store (the other project), rollback all on 1st error
func (p *Postgres) InsertBulk(ctx context.Context) error {
	tableCount := len(p.tables)
	if tableCount == 0 {
		return nil
	}
	tableChan := make(chan *Table, tableCount*tableCount)
	for i := 0; i < len(p.tables); i++ {
		tableChan <- &p.tables[i]
	}
	insertedTable := make(map[string]interface{})
tableInsertLoop:
	for t := range tableChan {
		// check if table has any parent table that is not yet seed
		if len(t.ParentTables) > 0 {
			for _, parent := range t.ParentTables {
				if parent == t.Name {
					// if a recursive relation found, ignore it
					continue
				}
				if _, ok := insertedTable[parent]; !ok {
					// if any parent table has not inserted yet, push the table back to tableChan
					p.log.Debug().Msgf("move table %s back to queue because the parent %s not found", t.Name, parent)
					tableChan <- t
					continue tableInsertLoop
				}
			}
		}
		p.log.Debug().Msgf("insert into table %s", t.Name)
		err := p.InsertBulkTable(ctx, *t)
		if err != nil {
			return err
		}
		insertedTable[t.Name] = struct{}{}
		if len(insertedTable) >= tableCount {
			break tableInsertLoop
		}
	}
	return nil
}

func (p *Postgres) withTx(ctx context.Context, act func(*sql.Tx) error) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	// TODO use recover from panic for stronger app guarantee
	defer func() {
		if err != nil {
			if err := tx.Rollback(); err != nil {
				p.log.Error().Err(err).Interface("tx", tx).Msg("failed to rollback tx")
			}
			return
		}
		if err := tx.Commit(); err != nil {
			p.log.Error().Err(err).Interface("tx", tx).Msg("failed to commit tx")
		}
	}()
	err = act(tx)
	return err
}

func (p *Postgres) InsertBulkTable(ctx context.Context, table Table) error {
	start := time.Now()
	var err error
	defer func() {
		msElapsed :=  time.Since(start).Milliseconds()
		if err != nil {
			p.log.Info().Err(err).
				Int64("ms_elapsed", msElapsed).
				Msgf("finished insert to table %s with error", table.Name)
		} else {
			p.log.Info().Int64("ms_elapsed", msElapsed).
				Msgf("finished insert to table %s successfully", table.Name)
		}
	}()
	stmt, ok := p.bulkInsertStmt[table.Name]
	if !ok {
		return fmt.Errorf("bulk insert prepared statement not found for table: %s", table.Name)
	}
	if table.Seeder == nil {
		return fmt.Errorf("table %s has no seeder", table.Name)
	}
	err = p.withTx(ctx, func(tx *sql.Tx) error {
		// TODO proper bulk insert
		for {
			row, hasData, err := table.Seeder.Next()
			if err != nil {
				return fmt.Errorf("failed to fetch next data from seeder: %w", err)
			}
			if !hasData {
				break
			}
			currentCount := table.Seeder.LatestDataCount()
			p.log.Debug().
				Int("current_data_count", currentCount).
				Str("table", table.Name).
				Interface("rows", row).Msg("seeding data")
			_, err = tx.StmtContext(ctx, stmt).ExecContext(ctx, row...)
			if err != nil {
				err = fmt.Errorf("failed bulk insert: %w", err)
				p.log.Error().
					Int("current_data_count", currentCount).
					Err(err).
					Str("table", table.Name).
					Interface("row", row).
					Msg("failed to insert data from seeder")

				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func NewPostgresFromConnectionString(
	connectionString string,
	tables []Table,
	log zerolog.Logger,
) (*Postgres, error) {
	conf, err := pgx.ParseConfig(connectionString)
	if err != nil {
		err = fmt.Errorf("failed to parse connection string: %w", err)
		return nil, dataFarmErr.Errorf(invalidConnectionStringError, err)
	}
	db := stdlib.OpenDB(*conf)
	err = db.Ping()
	if err != nil {
		err = fmt.Errorf("failed to ping db: %w", err)
		return nil, dataFarmErr.Errorf(failedToConnectDBError, err)
	}
	pg, err := NewPostgres(db, tables, log)
	if err != nil {
		return nil, err
	}
	return pg, nil
}

func NewPostgres(
	db *sql.DB,
	table []Table,
	log zerolog.Logger,
) (*Postgres, error) {
	p := &Postgres{
		db:     db,
		tables: table,
		log:    log.With().Str("rdbms", "postgres").Logger(),
	}
	if err := p.validateTables(); err != nil {
		return nil, dataFarmErr.Errorf(invalidTableSchemeError, err)
	}
	bulkInsertStmt := make(map[string]*sql.Stmt)
	for _, t := range table {
		str := t.bulkInsertStr()
		p.log.Debug().Str("stmt_str", str).Msg("bulk insert prepared statement str")
		stmt, err := p.db.Prepare(str)
		if err != nil {
			err = fmt.Errorf("failed to prepare bulk insert: %w", err)
			return nil, dataFarmErr.Errorf(failedToInitialiseDB, err)
		}
		bulkInsertStmt[t.Name] = stmt
	}
	p.bulkInsertStmt = bulkInsertStmt
	return p, nil
}

func (t *Table) bulkInsertStr() string {
	// TODO use string builder
	// TODO proper bulk insert
	columnNames := make([]string, len(t.Columns))
	param := make([]string, len(t.Columns))
	for idx, c := range t.Columns {
		columnNames[idx] = fmt.Sprintf(`"%s"`, c.Name)
		param[idx] = fmt.Sprintf("$%d", idx+1)
	}
	columnNameStr := strings.Join(columnNames, ",")
	paramStr := strings.Join(param, ",")
	str := fmt.Sprintf(`INSERT INTO "%s" (%s) VALUES (%s)`, t.Name, columnNameStr, paramStr)
	return str
}

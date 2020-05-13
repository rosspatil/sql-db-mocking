package mydb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// IFace represents the mydb capabilities-
// It is also used to generate mock test present in mock package
// User of this library also take the advantage of mock
type IFace interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)

	Close() error

	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)

	Ping() error
	PingContext(ctx context.Context) error

	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)

	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row

	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
}

// DB is a database library handle contains the master and read replica instances.
// It's safe for concurrent use by multiple goroutines.
// mydb package perform read operation on replica set and other operation on master.
type DB struct {
	IFace
	count        int
	master       *sql.DB
	readreplicas []*sql.DB
	m            sync.Mutex
}

// New returns a new instance of library handle i.e. DB
// at least one read replica instance is expected
func New(master *sql.DB, readreplicas ...*sql.DB) (*DB, error) {
	if len(readreplicas) == 0 {
		return nil, errors.New(noReadReplicaError)
	}
	return &DB{
		master:       master,
		m:            sync.Mutex{},
		readreplicas: readreplicas,
	}, nil
}

func (db *DB) readReplicaNumberRoundRobin() int {
	db.m.Lock()
	defer db.m.Unlock()
	db.count++
	return db.count % len(db.readreplicas)
}

// pingChanResponse is a response handler for ping channel
type pingChanResponse struct {
	err error
}

// Ping verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (db *DB) Ping() error {
	return db.PingContext(context.Background())
}

func (db *DB) ping(ctx context.Context, i int, pingChan chan pingChanResponse) {
	var e error
	if err := db.readreplicas[i].PingContext(ctx); err != nil {
		e = fmt.Errorf(replicaPingFailError, i+1, err.Error())
	}
	pingChan <- pingChanResponse{e}
}

// PingContext verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (db *DB) PingContext(ctx context.Context) error {
	var errString []string
	if err := db.master.PingContext(ctx); err != nil {
		e := fmt.Errorf(masterPingFailError, err.Error())
		errString = append(errString, e.Error())
	}

	// pingChan is used to listen the ping response from concurrent ping request for replicas
	pingChan := make(chan pingChanResponse, len(db.readreplicas))
	defer close(pingChan)
	for i := range db.readreplicas {
		go db.ping(ctx, i, pingChan)
	}

	for i := 0; i < len(db.readreplicas); i++ {
		chanResp, ok := <-pingChan
		if !ok {
			return errors.New(pingChannelCloseError)
		}
		if chanResp.err != nil {
			errString = append(errString, chanResp.err.Error())
		}
	}
	if len(errString) > 0 {
		return errors.New(strings.Join(errString, "\n"))
	}
	return nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
//
// This operation is performed on read replicas only
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
//
// This operation is performed on read replicas only.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	replicaIndex := db.readReplicaNumberRoundRobin()
	rows, err := db.readreplicas[replicaIndex].QueryContext(ctx, query, args...)
	if err == nil {
		return rows, err
	}
	// if selected replica is down or not alive for read request, Algorithm will select next available replica
	// for reading data in below lines
	// If all replicas are closed or not alive then error is return  "noReplicaAvailableError"
	for i := replicaIndex + 1; ; i++ {
		newIndex := i % len(db.readreplicas)
		if newIndex == replicaIndex {
			return nil, errors.New(noReplicaAvailableError)
		}
		rows, err := db.readreplicas[newIndex].QueryContext(ctx, query, args...)
		if err == nil {
			return rows, err
		}
	}
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
//
// QueryRow perform the query on replicas.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always returns a non-nil value. Errors are deferred until
// Row's Scan method is called.
// If the query selects no rows, the *Row's Scan will return ErrNoRows.
// Otherwise, the *Row's Scan scans the first selected row and discards
// the rest.
//
// QueryRowContext perform the query on replicas.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.readreplicas[db.readReplicaNumberRoundRobin()].QueryRowContext(ctx, query, args...)
}

// Begin starts a transaction on master db
func (db *DB) Begin() (*sql.Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

// BeginTx starts a transaction on master db.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.master.BeginTx(ctx, opts)
}

// Close returns the connection to the connection pool.
func (db *DB) Close() error {
	err := db.master.Close()
	for i := range db.readreplicas {
		err = db.readreplicas[i].Close()
	}
	return err
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
//
// Exec perform the query the on master db
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
//
// ExecContext perform the query the on master db
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.master.ExecContext(ctx, query, args...)
}

// Prepare creates a prepared statement for later queries or executions.
// The caller must call the statement's Close method
// when the statement is no longer needed.
//
// Prepare execute operation according to query. If query is for retrival of the data
// it will prepare statement on replica db, else it will be created on master db
func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement for later queries or executions.
// The caller must call the statement's Close method
// when the statement is no longer needed.
//
// PrepareContext execute operation according to query. If query is for retrival of the data
// it will prepare statement on replica db, else it will be created on master db
func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	// All the data retrival queries will be execute on replicas
	// If query is not for data retrival then only it is allow to execute on master db
	qSmall := strings.ToLower(strings.TrimSpace(query))
	if !strings.HasPrefix(qSmall, "select") {
		return db.master.PrepareContext(ctx, query)
	}
	return db.prepare(ctx, query)
}

func (db *DB) prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	replicaIndex := db.readReplicaNumberRoundRobin()
	stmt, err := db.readreplicas[replicaIndex].PrepareContext(ctx, query)
	if err == nil {
		return stmt, err
	}
	// if selected replica is down or not alive for read request, Algorithm will select next available replica
	// for reading data in below lines
	// If all replicas are closed or not alive then error is return  "noReplicaAvailableError"
	for i := replicaIndex + 1; ; i++ {
		newIndex := i % len(db.readreplicas)
		if newIndex == replicaIndex {
			return nil, errors.New(noReplicaAvailableError)
		}
		stmt, err := db.readreplicas[newIndex].PrepareContext(ctx, query)
		if err == nil {
			return stmt, err
		}
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
//
// If d <= 0, connections are reused forever.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.master.SetConnMaxLifetime(d)
	for i := range db.readreplicas {
		db.readreplicas[i].SetConnMaxLifetime(d)
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool.
//
// If n <= 0, no idle connections are retained.
//
// The default max idle connections is currently 2. This may change in
// a future release.
func (db *DB) SetMaxIdleConns(n int) {
	db.master.SetMaxIdleConns(n)
	for i := range db.readreplicas {
		db.readreplicas[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
//
// If n <= 0, then there is no limit on the number of open connections.
// The default is 0 (unlimited).
func (db *DB) SetMaxOpenConns(n int) {
	db.master.SetMaxOpenConns(n)
	for i := range db.readreplicas {
		db.readreplicas[i].SetMaxOpenConns(n)
	}
}

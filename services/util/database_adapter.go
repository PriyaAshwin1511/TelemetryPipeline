package util

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"telemetry-collector/services/config"
	"telemetry-collector/services/logger"

	_ "github.com/lib/pq"
)

// ConnectToDB establishes a connection to PostgreSQL with retry logic
func ConnectToDB(cfg *config.DatabaseConfig) (Database, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName,
	)

	var db *sql.DB
	var err error
	maxRetries := 3
	baseDelay := 3 * time.Second

	for i := 0; i < maxRetries; i++ {
		db, err = sql.Open("postgres", connStr)
		if err != nil {
			logger.Warnf("Failed to open database (attempt %d/%d): %v, retrying...", i+1, maxRetries, err)
			time.Sleep(baseDelay)
			continue
		}

		if err := db.Ping(); err != nil {
			logger.Warnf("Failed to ping database (attempt %d/%d): %v, retrying...", i+1, maxRetries, err)
			db.Close()
			time.Sleep(baseDelay)
			continue
		}

		db.SetMaxOpenConns(cfg.MaxOpenConns)
		db.SetMaxIdleConns(cfg.MaxIdleConns)

		logger.Infof("Connected to database (host=%s, port=%d, dbname=%s)", cfg.Host, cfg.Port, cfg.DBName)

		return NewSQLDB(db), nil
	}

	return nil, fmt.Errorf("failed to connect to database after %d attempts: %w", maxRetries, err)
}

// SQLDB is an adapter that wraps *sql.DB to implement the Database interface
type SQLDB struct {
	db *sql.DB
}

// NewSQLDB creates a new SQLDB adapter from *sql.DB
func NewSQLDB(db *sql.DB) Database {
	return &SQLDB{db: db}
}

// Query executes a query and returns the rows
func (s *SQLDB) Query(query string, args ...interface{}) (Rows, error) {
	return s.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query with context and returns the rows
func (s *SQLDB) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SQLRows{rows: rows}, nil
}

// QueryRow executes a query that is expected to return at most one row
func (s *SQLDB) QueryRow(query string, args ...interface{}) Row {
	return s.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext executes a query with context that is expected to return at most one row
func (s *SQLDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) Row {
	return &SQLRow{row: s.db.QueryRowContext(ctx, query, args...)}
}

// Exec executes a query without returning any rows
func (s *SQLDB) Exec(query string, args ...interface{}) (Result, error) {
	return s.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query with context without returning any rows
func (s *SQLDB) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SQLResult{result: result}, nil
}

// Ping checks if the database is accessible
func (s *SQLDB) Ping() error {
	return s.db.Ping()
}

// Close closes the database connection
func (s *SQLDB) Close() error {
	return s.db.Close()
}

// SetMaxOpenConns sets the maximum number of open connections
func (s *SQLDB) SetMaxOpenConns(n int) {
	s.db.SetMaxOpenConns(n)
}

// SetMaxIdleConns sets the maximum number of idle connections
func (s *SQLDB) SetMaxIdleConns(n int) {
	s.db.SetMaxIdleConns(n)
}

// SetConnMaxLifetime sets the maximum lifetime of a connection
func (s *SQLDB) SetConnMaxLifetime(d time.Duration) {
	s.db.SetConnMaxLifetime(d)
}

// Begin starts a transaction
func (s *SQLDB) Begin() (Tx, error) {
	return s.BeginTx(context.Background(), nil)
}

// BeginTx starts a transaction with context
func (s *SQLDB) BeginTx(ctx context.Context, opts interface{}) (Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &SQLTx{tx: tx}, nil
}

// SQLTx is an adapter that wraps *sql.Tx to implement the Tx interface
type SQLTx struct {
	tx *sql.Tx
}

// Exec executes a query within the transaction
func (s *SQLTx) Exec(query string, args ...interface{}) (Result, error) {
	return s.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query with context within the transaction
func (s *SQLTx) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	result, err := s.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SQLResult{result: result}, nil
}

// Query executes a query within the transaction
func (s *SQLTx) Query(query string, args ...interface{}) (Rows, error) {
	return s.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query with context within the transaction
func (s *SQLTx) QueryContext(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := s.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &SQLRows{rows: rows}, nil
}

// Prepare creates a prepared statement
func (s *SQLTx) Prepare(query string) (Stmt, error) {
	return s.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement with context
func (s *SQLTx) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt, err := s.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &SQLStmt{stmt: stmt}, nil
}

// Commit commits the transaction
func (s *SQLTx) Commit() error {
	return s.tx.Commit()
}

// Rollback rolls back the transaction
func (s *SQLTx) Rollback() error {
	return s.tx.Rollback()
}

// SQLRows is an adapter that wraps *sql.Rows to implement the Rows interface
type SQLRows struct {
	rows *sql.Rows
}

// Close closes the rows
func (s *SQLRows) Close() error {
	return s.rows.Close()
}

// Next advances to the next row
func (s *SQLRows) Next() bool {
	return s.rows.Next()
}

// Scan copies the columns in the current row into the values pointed at by dest
func (s *SQLRows) Scan(dest ...interface{}) error {
	return s.rows.Scan(dest...)
}

// Err returns any error that occurred while iterating
func (s *SQLRows) Err() error {
	return s.rows.Err()
}

// Columns returns the column names
func (s *SQLRows) Columns() ([]string, error) {
	return s.rows.Columns()
}

// SQLRow is an adapter that wraps *sql.Row to implement the Row interface
type SQLRow struct {
	row *sql.Row
}

// Scan copies the columns from the row into the values pointed at by dest
func (s *SQLRow) Scan(dest ...interface{}) error {
	return s.row.Scan(dest...)
}

// SQLResult is an adapter that wraps sql.Result to implement the Result interface
type SQLResult struct {
	result sql.Result
}

// LastInsertId returns the database-generated auto-incremented ID
func (s *SQLResult) LastInsertId() (int64, error) {
	return s.result.LastInsertId()
}

// RowsAffected returns the number of rows affected by the query
func (s *SQLResult) RowsAffected() (int64, error) {
	return s.result.RowsAffected()
}

// SQLStmt is an adapter that wraps *sql.Stmt to implement the Stmt interface
type SQLStmt struct {
	stmt *sql.Stmt
}

// Close closes the statement
func (s *SQLStmt) Close() error {
	return s.stmt.Close()
}

// Exec executes the prepared statement
func (s *SQLStmt) Exec(args ...interface{}) (Result, error) {
	return s.ExecContext(context.Background(), args...)
}

// ExecContext executes the prepared statement with context
func (s *SQLStmt) ExecContext(ctx context.Context, args ...interface{}) (Result, error) {
	result, err := s.stmt.ExecContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &SQLResult{result: result}, nil
}

// Query executes the prepared statement query
func (s *SQLStmt) Query(args ...interface{}) (Rows, error) {
	return s.QueryContext(context.Background(), args...)
}

// QueryContext executes the prepared statement query with context
func (s *SQLStmt) QueryContext(ctx context.Context, args ...interface{}) (Rows, error) {
	rows, err := s.stmt.QueryContext(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &SQLRows{rows: rows}, nil
}

// QueryRow executes the prepared statement query returning a single row
func (s *SQLStmt) QueryRow(args ...interface{}) Row {
	return s.QueryRowContext(context.Background(), args...)
}

// QueryRowContext executes the prepared statement query with context returning a single row
func (s *SQLStmt) QueryRowContext(ctx context.Context, args ...interface{}) Row {
	return &SQLRow{row: s.stmt.QueryRowContext(ctx, args...)}
}

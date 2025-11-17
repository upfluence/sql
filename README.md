# SQL Package Documentation

## Table of Contents

1. [Overview](#overview)
2. [Package Architecture](#package-architecture)
3. [Core Concepts](#core-concepts)
4. [Installation](#installation)
5. [Quick Start](#quick-start)
6. [Core API Reference](#core-api-reference)
7. [Database Operations](#database-operations)
8. [Transaction Management](#transaction-management)
9. [Advanced Features](#advanced-features)
10. [Query Builder](#query-builder)
11. [Database Migrations](#database-migrations)
12. [Error Handling](#error-handling)
13. [Middleware and Logging](#middleware-and-logging)
14. [Testing](#testing)
15. [Best Practices](#best-practices)
16. [FAQ](#faq)

---

## Overview

The `github.com/upfluence/sql` package provides a comprehensive, production-grade SQL database abstraction layer for Go applications. It abstracts multiple relational database backends (PostgreSQL, SQLite3) through a unified interface, enabling seamless database portability while supporting advanced features including:

- **Multiple Backend Support**: Unified interface for PostgreSQL and SQLite3
- **Master-Slave Replication**: Automatic routing of queries to appropriate database replicas
- **Load Balancing**: Distributing queries across multiple database instances
- **Connection Pooling**: Optimized connection management with configurable parameters
- **Transaction Management**: Automatic retry logic for concurrent transaction conflicts
- **SQL Query Builder**: Type-safe, fluent API for constructing SQL statements
- **Database Migrations**: File system-based schema versioning and application
- **Comprehensive Error Classification**: Distinguishing constraint violations from transient errors
- **Middleware Support**: Query logging and extensible cross-cutting concerns
- **Custom SQL Types**: Enhanced nullable types and specialized data structures

The package is designed for building scalable, maintainable database-driven applications with emphasis on correctness, performance, and operational visibility.

---

## Package Architecture

### Layered Architecture Overview

```mermaid
graph TD
    A["Application Code"]
    B["sqlutil.Open()"]
    C["Master/Slave<br/>Configuration"]
    D["Replication<br/>Strategy"]
    E["Load Balancer<br/>Strategy"]
    F["Middleware<br/>Query Logging"]
    G["PostgreSQL<br/>Backend"]
    H["SQLite3<br/>Backend"]
    I["Static<br/>Backend"]
    J["database/sql<br/>Interface"]

    A --> B
    B --> C
    B --> D
    B --> E
    C --> F
    D --> F
    E --> F
    F --> G
    F --> H
    F --> I
    G --> J
    H --> J
    I --> J

    style A fill:#e1f5ff
    style B fill:#fff9c4
    style F fill:#f3e5f5
    style G fill:#e8f5e9
    style H fill:#e8f5e9
    style I fill:#e8f5e9
    style J fill:#fce4ec
```

### Core Package Modules

| Module | Purpose | Key Types |
|--------|---------|-----------|
| `sql` | Core interfaces and abstractions | DB, Tx, Queryer, Cursor, Scanner |
| `sql/backend/postgres` | PostgreSQL implementation | postgres.DB |
| `sql/backend/sqlite3` | SQLite3 implementation | sqlite3.DB |
| `sql/backend/simple` | Basic wrapper | simple.DB |
| `sql/backend/replication` | Master-slave routing | replication.DB |
| `sql/backend/balancer` | Load balancing framework | balancer.DB |
| `sql/sqlutil` | Database initialization | Open(), DBOption |
| `sql/x/sqlbuilder` | Query construction | QueryBuilder, SelectStatement, InsertStatement |
| `sql/x/migration` | Schema migration | Migrator, Migration, FSSource |
| `sql/middleware/logger` | Query logging | Factory, LevelFactory, DebugFactory |
| `sql/sqltest` | Testing utilities | TestCase, Migrations |
| `sql/sqltypes` | Enhanced SQL types | JSONValue, NullUTCTime, StringSlice |

---

## Core Concepts

### Database Interface

The `DB` interface represents a database connection and is the primary entry point for all database operations:

```go
type DB interface {
    // Query execution methods
    Exec(context.Context, string, ...interface{}) (Result, error)
    QueryRow(context.Context, string, ...interface{}) Scanner
    Query(context.Context, string, ...interface{}) (Cursor, error)

    // Transaction support
    BeginTx(context.Context, TxOptions) (Tx, error)

    // Driver identification
    Driver() string
}
```

### Transaction Interface

The `Tx` interface extends `Queryer` with commit/rollback semantics:

```go
type Tx interface {
    Queryer
    Commit() error
    Rollback() error
}
```

### Isolation Levels

The package supports standard SQL isolation levels for transaction configuration:

```go
const (
    LevelDefault          IsolationLevel = iota
    LevelReadUncommitted
    LevelReadCommitted
    LevelWriteCommitted
    LevelRepeatableRead
    LevelSnapshot
    LevelSerializable
    LevelLinearizable
)
```

### Query Results

The `Cursor` interface provides row-by-row iteration:

```go
type Cursor interface {
    Next() bool
    Close() error
    Err() error
    Scan(...interface{}) error
}
```

### Consistency Levels

For master-slave replication scenarios, specify consistency guarantees:

```go
const (
    EventuallyConsistent Consistency = iota  // May read from slave
    StronglyConsistent                       // Read from master
)
```

---

## Installation

### Prerequisites

- Go 1.23 or higher
- For PostgreSQL support: PostgreSQL 11+
- For SQLite3 support: SQLite3 3.27+

### Getting the Package

```bash
go get github.com/upfluence/sql
```

### Importing

```go
import "github.com/upfluence/sql"
import "github.com/upfluence/sql/x/sqlbuilder"
import "github.com/upfluence/sql/x/migration"
```

---

## Quick Start

### Basic Database Connection

```go
package main

import (
    "context"
    "log"

    "github.com/upfluence/sql/sqlutil"
)

func main() {
    ctx := context.Background()

    // Open a PostgreSQL database
    db, err := sqlutil.Open(ctx, "postgres://user:password@localhost/mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Now use db for queries...
}
```

### Executing Queries

```go
// Simple query with row result
var name string
row := db.QueryRow(ctx, "SELECT name FROM users WHERE id = $1", userID)
if err := row.Scan(&name); err != nil {
    // Handle error
}

// Query multiple rows
rows, err := db.Query(ctx, "SELECT id, name FROM users")
if err != nil {
    // Handle error
}
defer rows.Close()

for rows.Next() {
    var id int
    var name string
    if err := rows.Scan(&id, &name); err != nil {
        // Handle error
        continue
    }
    // Process row
}

// Executing statements (INSERT, UPDATE, DELETE)
result, err := db.Exec(ctx,
    "INSERT INTO users (name, email) VALUES ($1, $2)",
    name, email)
if err != nil {
    // Handle error
}

rowsAffected, err := result.RowsAffected()
```

### Basic Transaction

```go
err := sql.ExecuteTx(ctx, db, sql.TxOptions{}, func(tx sql.Queryer) error {
    // Execute queries within transaction
    result, err := tx.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", name)
    if err != nil {
        return err
    }

    // Query within transaction
    var count int
    if err := tx.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
        return err
    }

    return nil  // Commit automatically on nil return
})
```

---

## Core API Reference

### Database Opening

#### `sqlutil.Open()`

Opens and initializes a database connection with automatic backend selection and optional replication/load balancing configuration.

**Signature:**
```go
func Open(
    ctx context.Context,
    dataSourceName string,
    opts ...Option,
) (sql.DB, error)
```

**Parameters:**
- `ctx`: Context for initialization
- `dataSourceName`: Connection string (PostgreSQL: `postgres://...`, SQLite3: `file:...`)
- `opts`: Variable options (WithMaster, WithSlave, WithMiddleware, etc.)

**Returns:**
- `sql.DB`: Configured database interface
- `error`: Initialization error if any

**Example:**
```go
// Simple single database
db, err := sqlutil.Open(ctx, "postgres://localhost/mydb")

// With connection pool configuration
db, err := sqlutil.Open(ctx, "postgres://localhost/mydb",
    sqlutil.WithMaxOpenConns(100),
    sqlutil.WithMaxIdleConns(10),
    sqlutil.WithConnMaxIdleTime(5*time.Minute),
)

// With master-slave replication
db, err := sqlutil.Open(ctx, "postgres://localhost/primary_db",
    sqlutil.WithSlave("postgres://replica1/mydb"),
    sqlutil.WithSlave("postgres://replica2/mydb"),
)

// With load balancing
db, err := sqlutil.Open(ctx, "postgres://db1/mydb",
    sqlutil.WithBalancer(
        "postgres://db2/mydb",
        "postgres://db3/mydb",
    ),
)

// With logging middleware
db, err := sqlutil.Open(ctx, "postgres://localhost/mydb",
    sqlutil.WithMiddleware(logger.NewDebugFactory()),
)
```

### Query Execution

#### `DB.Exec()`

Executes a SQL statement and returns a Result object containing information about the operation.

**Signature:**
```go
func (db DB) Exec(
    ctx context.Context,
    query string,
    args ...interface{},
) (sql.Result, error)
```

**Example:**
```go
result, err := db.Exec(ctx,
    "INSERT INTO users (name, email, created_at) VALUES ($1, $2, $3)",
    "John Doe",
    "john@example.com",
    time.Now(),
)
if err != nil {
    // Handle error
}

lastID, err := result.LastInsertId()      // PostgreSQL: may not be supported
affectedRows, err := result.RowsAffected()
```

#### `DB.QueryRow()`

Executes a query that returns a single row.

**Signature:**
```go
func (db DB) QueryRow(
    ctx context.Context,
    query string,
    args ...interface{},
) sql.Scanner
```

**Example:**
```go
var userID int
var email string

scanner := db.QueryRow(ctx,
    "SELECT id, email FROM users WHERE username = $1",
    username)

if err := scanner.Scan(&userID, &email); err != nil {
    if err == sql.ErrNoRows {
        // User not found
    }
    // Handle error
}
```

#### `DB.Query()`

Executes a query that returns multiple rows.

**Signature:**
```go
func (db DB) Query(
    ctx context.Context,
    query string,
    args ...interface{},
) (sql.Cursor, error)
```

**Example:**
```go
rows, err := db.Query(ctx,
    "SELECT id, name, email FROM users WHERE active = $1",
    true)
if err != nil {
    // Handle error
}
defer rows.Close()

for rows.Next() {
    var id int
    var name, email string

    if err := rows.Scan(&id, &name, &email); err != nil {
        // Handle scan error
        continue
    }

    // Process row data
    fmt.Printf("User: %d, %s, %s\n", id, name, email)
}

if err := rows.Err(); err != nil {
    // Handle iteration error
}
```

#### `sql.ScrollCursor()`

Helper function to iterate over cursor results using a callback function.

**Signature:**
```go
func ScrollCursor(
    cursor sql.Cursor,
    fn func() error,
) error
```

**Example:**
```go
rows, err := db.Query(ctx, "SELECT id, name FROM users")
if err != nil {
    log.Fatal(err)
}

err = sql.ScrollCursor(rows, func() error {
    var id int
    var name string

    if err := rows.Scan(&id, &name); err != nil {
        return err
    }

    fmt.Printf("%d: %s\n", id, name)
    return nil
})
```

---

## Database Operations

### Connection Pool Configuration

The `sqlutil` package provides fine-grained control over connection pooling parameters:

#### `WithMaxOpenConns()`

Sets the maximum number of open connections to the database.

**Default:** 128 connections

```go
db, err := sqlutil.Open(ctx, dsn,
    sqlutil.WithMaxOpenConns(256),
)
```

#### `WithMaxIdleConns()`

Sets the maximum number of idle connections in the pool.

**Default:** 16 connections

```go
db, err := sqlutil.Open(ctx, dsn,
    sqlutil.WithMaxIdleConns(20),
)
```

#### `WithConnMaxIdleTime()`

Sets the maximum amount of time a connection can remain idle before being closed.

**Default:** 2 minutes

```go
db, err := sqlutil.Open(ctx, dsn,
    sqlutil.WithConnMaxIdleTime(5*time.Minute),
)
```

#### `WithConnMaxLifetime()`

Sets the maximum lifetime of a connection.

```go
db, err := sqlutil.Open(ctx, dsn,
    sqlutil.WithConnMaxLifetime(30*time.Minute),
)
```

### Parameterized Queries

Always use parameterized queries to prevent SQL injection:

```go
// ✓ GOOD: Parameterized query
rows, err := db.Query(ctx,
    "SELECT * FROM users WHERE email = $1",
    userEmail)

// ✗ BAD: String concatenation (SQL injection vulnerability)
rows, err := db.Query(ctx,
    fmt.Sprintf("SELECT * FROM users WHERE email = '%s'", userEmail))
```

### Query Options

The package supports query-level options via variadic arguments:

#### Consistency Level Option

Specify consistency requirements for queries in replicated environments:

```go
// Query may read from any slave
rows, err := db.Query(ctx,
    "SELECT * FROM users WHERE id = $1",
    userID,
    sql.EventuallyConsistent,
)

// Query must read from master (strongly consistent)
rows, err := db.Query(ctx,
    "SELECT * FROM users WHERE id = $1",
    userID,
    sql.StronglyConsistent,
)
```

#### Option Filtering

Remove options from arguments for backend-specific processing:

```go
// The StripOptions function removes all Option values
// useful when passing arguments to lower-level implementations
cleanArgs := sql.StripOptions(args)
```

---

## Transaction Management

### Basic Transactions

Transactions guarantee ACID properties and are initiated via `DB.BeginTx()`:

```go
tx, err := db.BeginTx(ctx, sql.TxOptions{
    Isolation: sql.LevelReadCommitted,
})
if err != nil {
    return err
}

// Execute queries on transaction
result, err := tx.Exec(ctx, "INSERT INTO users (name) VALUES ($1)", name)
if err != nil {
    tx.Rollback()
    return err
}

// Commit the transaction
if err := tx.Commit(); err != nil {
    return err
}
```

### Automatic Transaction Retry

The `ExecuteTx()` function provides automatic retry logic for transient failures:

```go
err := sql.ExecuteTx(ctx, db, sql.TxOptions{
    Isolation: sql.LevelSerializable,
}, func(tx sql.Queryer) error {
    // Transaction logic here
    // If this returns a retryable error, ExecuteTx automatically retries

    // Insert a user
    _, err := tx.Exec(ctx,
        "INSERT INTO users (name, email) VALUES ($1, $2)",
        "Alice", "alice@example.com")
    if err != nil {
        return err
    }

    // Query to verify
    var count int
    if err := tx.QueryRow(ctx,
        "SELECT COUNT(*) FROM users WHERE email = $1",
        "alice@example.com").Scan(&count); err != nil {
        return err
    }

    return nil  // Automatically committed
})
```

### Retry Configuration

#### `WithRetryCount()`

Configure maximum number of retry attempts:

```go
err := sql.ExecuteTx(ctx, db, sql.TxOptions{},
    func(tx sql.Queryer) error {
        // transaction logic
        return nil
    },
    sql.WithRetryCount(3),  // Maximum 3 attempts
)
```

Default: `sql.InfiniteRetry` (unlimited retries for serialization conflicts)

#### `WithCustomRetryCheck()`

Provide custom logic to determine if an error is retryable:

```go
err := sql.ExecuteTx(ctx, db, sql.TxOptions{},
    func(tx sql.Queryer) error {
        // transaction logic
        return nil
    },
    sql.WithCustomRetryCheck(func(err error) bool {
        // Custom logic to determine if retryable
        var rollbackErr sql.RollbackError
        if errors.As(err, &rollbackErr) {
            return rollbackErr.Type == sql.SerializationFailure
        }
        return false
    }),
)
```

### Rollback Handling

Explicit rollback without error:

```go
err := sql.ExecuteTx(ctx, db, sql.TxOptions{},
    func(tx sql.Queryer) error {
        // Execute logic...

        // Explicit rollback without error propagation
        return sql.ErrRollback
    },
)
// err will be nil if explicit rollback succeeds
```

### Isolation Levels

Choose appropriate isolation level for your consistency requirements:

| Level | Description | Use Case |
|-------|-------------|----------|
| `LevelDefault` | Backend default (typically READ COMMITTED) | Most general workloads |
| `LevelReadUncommitted` | Dirty reads allowed | Read-only analytics, performance critical |
| `LevelReadCommitted` | Prevents dirty reads | Most OLTP transactions |
| `LevelRepeatableRead` | Prevents dirty and non-repeatable reads | Complex multi-row operations |
| `LevelSerializable` | Strongest isolation | Critical financial transactions |

**Isolation Level Guide:**

```go
// For high-frequency, simple updates
err := sql.ExecuteTx(ctx, db, sql.TxOptions{
    Isolation: sql.LevelReadCommitted,
}, txFunc)

// For operations involving multiple related rows
err := sql.ExecuteTx(ctx, db, sql.TxOptions{
    Isolation: sql.LevelRepeatableRead,
}, txFunc)

// For critical financial transactions
err := sql.ExecuteTx(ctx, db, sql.TxOptions{
    Isolation: sql.LevelSerializable,
}, txFunc)
```

---

## Advanced Features

### Master-Slave Replication

Automatically route read queries to slave replicas and write queries to master:

```go
db, err := sqlutil.Open(ctx, "postgres://master/mydb",
    sqlutil.WithSlave("postgres://slave1/mydb"),
    sqlutil.WithSlave("postgres://slave2/mydb"),
)
```

**Query Routing:**
- `SELECT` queries → directed to slave replicas
- `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER` → directed to master
- Transactions → always use master

**Force Master Reads:**

```go
db, err := sqlutil.Open(ctx, "postgres://master/mydb",
    sqlutil.WithSlave("postgres://slave1/mydb"),
    sqlutil.UseMasterForReads,  // All queries use master
)
```

### Load Balancing

Distribute queries across multiple database instances:

```go
db, err := sqlutil.Open(ctx, "postgres://db1/mydb",
    sqlutil.WithBalancer(
        "postgres://db2/mydb",
        "postgres://db3/mydb",
    ),
)
```

Default balancing strategy is round-robin. Custom balancers can be implemented by wrapping `sql.DB` instances.

### Custom Driver Wrappers

Register custom database drivers:

```go
import "github.com/upfluence/sql/sqlutil"

// Implement custom wrapping logic
func customWrapper(db sql.DB, parser sqlparser.SQLParser) sql.DB {
    // Wrap database with custom logic
    return customDB{
        underlying: db,
        parser:     parser,
    }
}

// Register before using
sqlutil.RegisterDriverWrapper("custom", customWrapper)

// Now use in Open
db, err := sqlutil.Open(ctx, "custom://...")
```

---

## Query Builder

The `sqlbuilder` package provides a fluent, type-safe API for constructing SQL statements:

### SELECT Queries

```go
import "github.com/upfluence/sql/x/sqlbuilder"

// Simple SELECT
stmt := sqlbuilder.PrepareSelect("users").
    Columns("id", "name", "email").
    Where(sqlbuilder.Eq("status", "active"))

query, args, err := stmt.ToSql()
// query: "SELECT id, name, email FROM users WHERE status = $1"
// args: []interface{}{"active"}
```

#### Complex Selections

```go
stmt := sqlbuilder.PrepareSelect("users").
    Columns("id", "name", "email", "created_at").
    Where(
        sqlbuilder.And(
            sqlbuilder.Eq("status", "active"),
            sqlbuilder.Gt("created_at", createdAfter),
        ),
    ).
    OrderBy("created_at", "DESC").
    Limit(50).
    Offset(0)

query, args, err := stmt.ToSql()

// Execute query
rows, err := db.Query(ctx, query, args...)
```

#### JOINs

```go
stmt := sqlbuilder.PrepareSelect("orders o").
    Columns("o.id", "o.total", "u.name").
    Join(sqlbuilder.InnerJoin("users u").On(
        sqlbuilder.Eq("u.id", "o.user_id"),
    )).
    Where(sqlbuilder.Gt("o.total", 100))

query, args, err := stmt.ToSql()
```

#### Aggregations

```go
stmt := sqlbuilder.PrepareSelect("orders").
    Columns("user_id", "COUNT(*) as order_count", "SUM(total) as total_amount").
    GroupBy("user_id").
    Having(sqlbuilder.Gt("COUNT(*)", 5))

query, args, err := stmt.ToSql()
```

### INSERT Queries

```go
stmt := sqlbuilder.PrepareInsert("users").
    Columns("name", "email", "created_at").
    Values("John Doe", "john@example.com", time.Now())

query, args, err := stmt.ToSql()
// query: "INSERT INTO users (name, email, created_at) VALUES ($1, $2, $3)"

// Execute
result, err := db.Exec(ctx, query, args...)
```

#### INSERT with RETURNING (PostgreSQL)

```go
stmt := sqlbuilder.PrepareInsert("users").
    Columns("name", "email").
    Values("Alice", "alice@example.com").
    Returning("id", "created_at")

query, args, err := stmt.ToSql()
// query: "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, created_at"

// Get inserted ID
var userID int
var createdAt time.Time
row := db.QueryRow(ctx, query, args...)
if err := row.Scan(&userID, &createdAt); err != nil {
    log.Fatal(err)
}
```

#### ON CONFLICT Handling (PostgreSQL)

```go
stmt := sqlbuilder.PrepareInsert("users").
    Columns("id", "name", "email").
    Values(100, "Bob", "bob@example.com").
    OnConflict(
        sqlbuilder.NewOnConflictClause().
            Target("id").
            DoUpdate(
                sqlbuilder.Eq("name", "Bob"),
                sqlbuilder.Eq("email", "bob@example.com"),
            ),
    )

query, args, err := stmt.ToSql()
```

### UPDATE Queries

```go
stmt := sqlbuilder.PrepareUpdate("users").
    Set("name", "Updated Name").
    Set("updated_at", time.Now()).
    Where(sqlbuilder.Eq("id", userID))

query, args, err := stmt.ToSql()
// query: "UPDATE users SET name = $1, updated_at = $2 WHERE id = $3"

result, err := db.Exec(ctx, query, args...)
```

### DELETE Queries

```go
stmt := sqlbuilder.PrepareDelete("users").
    Where(sqlbuilder.Eq("status", "inactive"))

query, args, err := stmt.ToSql()
// query: "DELETE FROM users WHERE status = $1"

result, err := db.Exec(ctx, query, args...)
affectedRows, _ := result.RowsAffected()
```

### Query Predicates

The package provides a comprehensive set of predicate builders:

```go
// Equality and comparison
sqlbuilder.Eq("column", value)           // column = value
sqlbuilder.Neq("column", value)          // column != value
sqlbuilder.Gt("column", value)           // column > value
sqlbuilder.Gte("column", value)          // column >= value
sqlbuilder.Lt("column", value)           // column < value
sqlbuilder.Lte("column", value)          // column <= value

// String operations
sqlbuilder.Like("column", pattern)       // column LIKE pattern
sqlbuilder.ILike("column", pattern)      // column ILIKE pattern
sqlbuilder.In("column", val1, val2)      // column IN (val1, val2)
sqlbuilder.NotIn("column", val1, val2)   // column NOT IN (val1, val2)

// NULL handling
sqlbuilder.IsNull("column")              // column IS NULL
sqlbuilder.IsNotNull("column")           // column IS NOT NULL

// Logical operators
sqlbuilder.And(pred1, pred2)             // pred1 AND pred2
sqlbuilder.Or(pred1, pred2)              // pred1 OR pred2
sqlbuilder.Not(pred)                     // NOT pred

// Array operations (PostgreSQL)
sqlbuilder.ArrayContains("column", val)  // column @> ARRAY[val]
sqlbuilder.Overlaps("column", val)       // column && ARRAY[val]
```

---

## Database Migrations

The `migration` package provides file system-based schema versioning:

### Migration File Format

Place migration files in a directory with naming convention:

```
migrations/
├── 001_create_users_table.up.sql
├── 001_create_users_table.down.sql
├── 002_add_email_index.up.sql
├── 002_add_email_index.down.sql
└── 003_create_orders_table.up.sql
```

### Running Migrations

```go
import "github.com/upfluence/sql/x/migration"

// Create migrator from file system
source, err := migration.NewFSSource("./migrations")
if err != nil {
    log.Fatal(err)
}

migrator, err := migration.NewMigrator(db, source)
if err != nil {
    log.Fatal(err)
}

// Apply all pending migrations
if err := migrator.Migrate(ctx); err != nil {
    log.Fatal(err)
}
```

### Migration Management

```go
// Get current migration version
version, err := migrator.Version(ctx)

// Rollback to specific version
if err := migrator.Rollback(ctx, version-1); err != nil {
    log.Fatal(err)
}

// Rollback one step
if err := migrator.RollbackOne(ctx); err != nil {
    log.Fatal(err)
}
```

### Migration SQL Examples

**001_create_users_table.up.sql:**
```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_users_email ON users(email);
```

**002_add_email_verification.up.sql:**
```sql
ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN verified_at TIMESTAMP NULL;
```

---

## Error Handling

### Error Types

The package provides typed errors for specific failure scenarios:

#### ConstraintError

Indicates database constraint violations:

```go
import "github.com/upfluence/errors"
import "github.com/upfluence/sql"

result, err := db.Exec(ctx,
    "INSERT INTO users (id, email) VALUES ($1, $2)",
    duplicateID, email)

if err != nil {
    var constraintErr sql.ConstraintError
    if errors.As(err, &constraintErr) {
        switch constraintErr.Type {
        case sql.PrimaryKey:
            fmt.Println("Duplicate primary key")
        case sql.ForeignKey:
            fmt.Println("Foreign key violation")
        case sql.NotNull:
            fmt.Println("Not null constraint violated")
        case sql.Unique:
            fmt.Println("Unique constraint violated")
        }
    }
}
```

#### RollbackError

Indicates transient transaction failures:

```go
var rollbackErr sql.RollbackError
if errors.As(err, &rollbackErr) {
    switch rollbackErr.Type {
    case sql.SerializationFailure:
        // Transaction conflict - can be retried
        fmt.Println("Serialization conflict - retrying")
    case sql.Locked:
        // Resource locked - can be retried
        fmt.Println("Resource locked - retrying")
    }
}
```

### Standard Errors

The package re-exports standard library errors:

```go
import "github.com/upfluence/sql"

rows, err := db.Query(ctx, "SELECT * FROM users")

if err == sql.ErrNoRows {
    fmt.Println("No results found")
}

if err == sql.ErrConnDone {
    fmt.Println("Connection already closed")
}

if err == sql.ErrTxDone {
    fmt.Println("Transaction already committed or rolled back")
}
```

### Error Wrapping Pattern

Always wrap errors with context:

```go
import "github.com/upfluence/errors"

result, err := db.Exec(ctx, query, args...)
if err != nil {
    return errors.Wrap(err, "failed to insert user record")
}
```

---

## Middleware and Logging

### Query Logging

Add comprehensive query logging to track SQL execution:

```go
import "github.com/upfluence/sql/middleware/logger"

// Debug factory logs all queries with arguments
db, err := sqlutil.Open(ctx, dsn,
    sqlutil.WithMiddleware(logger.NewDebugFactory()),
)

// Level-based factory with configurable severity
db, err := sqlutil.Open(ctx, dsn,
    sqlutil.WithMiddleware(
        logger.NewLevelFactory(logLevel),
    ),
)
```

### Multiple Middleware

Chain multiple middleware for layered functionality:

```go
db, err := sqlutil.Open(ctx, dsn,
    sqlutil.WithMiddleware(logger.NewDebugFactory()),
    sqlutil.WithMiddleware(metricsFactory),  // Custom metrics middleware
)
```

### Custom Middleware

Implement custom middleware by wrapping the DB interface:

```go
type queryTracker struct {
    underlying sql.DB
    metrics    *MetricsCollector
}

func (qt *queryTracker) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
    start := time.Now()
    defer func() {
        duration := time.Since(start)
        qt.metrics.RecordQueryDuration("exec", duration)
    }()

    return qt.underlying.Exec(ctx, query, args...)
}

// Implement other DB interface methods...
```

---

## Testing

### Testing with Multiple Backends

The `sqltest` package provides `TestCase` for running tests across database backends:

```go
import "github.com/upfluence/sql/sqltest"

func TestUserRepository(t *testing.T) {
    testCase := sqltest.NewTestCase(t,
        // PostgreSQL test database
        "postgres://localhost/test_db",
        // SQLite3 in-memory database
        "file::memory:",
    )

    for _, tc := range testCase.Cases() {
        t.Run(tc.Name, func(t *testing.T) {
            db := tc.DB

            // Your test logic
            result, err := db.Exec(context.Background(),
                "INSERT INTO users (name) VALUES ($1)",
                "Test User")
            assert.NoError(t, err)
            assert.NotNil(t, result)
        })
    }
}
```

### Mock Database

Use static database for testing without real database:

```go
import "github.com/upfluence/sql/backend/static"

mockDB := static.NewDB(
    static.WithCursor([]interface{}{1, "John", "john@example.com"}),
)

rows, err := mockDB.Query(ctx, "SELECT * FROM users")
// Cursor returns predefined data
```

---

## Best Practices

### 1. Connection Pool Configuration

```go
// Production configuration
db, err := sqlutil.Open(ctx, dsn,
    sqlutil.WithMaxOpenConns(100),
    sqlutil.WithMaxIdleConns(20),
    sqlutil.WithConnMaxIdleTime(5*time.Minute),
    sqlutil.WithConnMaxLifetime(30*time.Minute),
)
```

**Guidelines:**
- `MaxOpenConns`: 10x average concurrent queries
- `MaxIdleConns`: 20-30% of MaxOpenConns
- `ConnMaxIdleTime`: 2-5 minutes for cloud databases, 5-10 for on-premises
- `ConnMaxLifetime`: 30 minutes to force connection refreshing

### 2. Always Use Context

```go
// ✓ GOOD: Pass context for cancellation/timeout
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

result, err := db.Exec(ctx, query, args...)

// ✗ BAD: No timeout
result, err := db.Exec(context.Background(), query, args...)
```

### 3. Parameterized Queries

```go
// ✓ GOOD: Use parameters
db.Query(ctx, "SELECT * FROM users WHERE email = $1", userEmail)

// ✗ BAD: String concatenation (SQL injection)
db.Query(ctx, fmt.Sprintf("SELECT * FROM users WHERE email = '%s'", userEmail))
```

### 4. Defer Row Closing

```go
rows, err := db.Query(ctx, query)
if err != nil {
    return err
}
defer rows.Close()  // Always defer Close()

for rows.Next() {
    // Process rows
}
```

### 5. Retry Strategies

```go
// Automatically retry on serialization failures
err := sql.ExecuteTx(ctx, db, sql.TxOptions{
    Isolation: sql.LevelSerializable,
}, func(tx sql.Queryer) error {
    // Transaction logic - will auto-retry on conflicts
    return performDatabaseOperations(tx)
})
```

### 6. Consistency Levels in Replicated Setup

```go
// For read-heavy workloads (eventually consistent acceptable)
rows, err := db.Query(ctx,
    "SELECT * FROM users WHERE status = 'active'",
    sql.EventuallyConsistent)

// For critical operations (strongly consistent)
var balance int
db.QueryRow(ctx,
    "SELECT balance FROM accounts WHERE id = $1",
    accountID,
    sql.StronglyConsistent).Scan(&balance)
```

### 7. Transaction Error Handling

```go
err := sql.ExecuteTx(ctx, db, sql.TxOptions{
    Isolation: sql.LevelReadCommitted,
}, func(tx sql.Queryer) error {
    result, err := tx.Exec(ctx, query, args...)
    if err != nil {
        // Automatic rollback on error
        return err
    }

    // Return nil to commit
    return nil
})

if err != nil {
    // Handle transaction failure
    log.Printf("Transaction failed: %v", err)
}
```

### 8. Logging Configuration

```go
// Development: Debug all queries
db, _ := sqlutil.Open(ctx, dsn,
    sqlutil.WithMiddleware(logger.NewDebugFactory()),
)

// Production: Log warnings and errors only
db, _ := sqlutil.Open(ctx, dsn,
    sqlutil.WithMiddleware(logger.NewLevelFactory(logrus.WarnLevel)),
)
```

### 9. Resource Cleanup

```go
db, err := sqlutil.Open(ctx, dsn)
if err != nil {
    return err
}
defer func() {
    if err := db.Close(); err != nil {
        log.Printf("Failed to close database: %v", err)
    }
}()

// Use database
```

### 10. Migration Versioning

```go
// Always check current version before running migrations
version, err := migrator.Version(ctx)
if err != nil {
    // Handle error
}

if version < targetVersion {
    if err := migrator.Migrate(ctx); err != nil {
        return err
    }
}
```

---

## FAQ

### Q: Should I use SQLite3 or PostgreSQL?

**A:** Choose based on use case:
- **PostgreSQL**: Production deployments, high concurrency, ACID guarantees required
- **SQLite3**: Development, testing, embedded/offline scenarios, single-process applications

### Q: How do I handle database connection failures?

**A:** The package retries automatically via connection pooling. For application-level handling:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

rows, err := db.Query(ctx, query)
if err != nil {
    if ctx.Err() == context.DeadlineExceeded {
        // Query timeout
    } else {
        // Other connection/query error
    }
}
```

### Q: Can I use prepared statements?

**A:** The package handles query preparation internally for consistency. Use the query builder for complex statements:

```go
stmt := sqlbuilder.PrepareSelect("users").
    Columns("id", "name").
    Where(sqlbuilder.Eq("email", "$1"))

query, args, _ := stmt.ToSql()
rows, _ := db.Query(ctx, query, args...)
```

### Q: How do I handle data type conversions?

**A:** The package provides special types in `sqltypes`:

```go
import "github.com/upfluence/sql/sqltypes"

type User struct {
    ID        int
    Email     string
    Metadata  sqltypes.JSONValue  // Nullable JSON
    CreatedAt sqltypes.NullUTCTime // Nullable UTC time
    Tags      sqltypes.StringSlice // Array of strings
}
```

### Q: What is the performance impact of middleware?

**A:** Middleware adds minimal overhead (~1-2% for logging). For latency-sensitive operations, consider conditional middleware or custom implementations that only log certain queries.

### Q: How do I optimize slow queries?

**A:** Use logging middleware to identify slow queries:

```go
db, _ := sqlutil.Open(ctx, dsn,
    sqlutil.WithMiddleware(logger.NewDebugFactory()),
)

// Examine logs for queries taking > X ms
```

Then:
1. Add database indexes on WHERE clause columns
2. Use EXPLAIN ANALYZE to understand query plans
3. Consider denormalization or caching for expensive aggregations

### Q: Should I use transactions for all queries?

**A:** No. Use transactions only when:
- Multiple queries must succeed or fail atomically
- Consistency between related rows is critical
- You need isolation from concurrent modifications

### Q: How do I migrate from one database to another?

**A:** Use the `sqltest.TestCase` to ensure migrations work on both databases, then:

```go
// Run migration on both old and new databases
migrator := migration.NewMigrator(newDB, source)
if err := migrator.Migrate(ctx); err != nil {
    return err
}

// Verify data integrity before cutover
```

### Q: What isolation level should I use by default?

**A:** `LevelReadCommitted` is safe for most OLTP workloads. Upgrade to `LevelSerializable` only for transactions with:
- Complex multi-row operations
- Financial implications
- Need for absolute consistency

---

## Related Resources

- [Go database/sql Documentation](https://golang.org/pkg/database/sql/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [SQLite3 Documentation](https://www.sqlite.org/docs.html)
- [Transaction Patterns and Best Practices](https://www.postgresql.org/docs/current/transaction-iso.html)

---

**Version:** 1.0.0 | **Last Updated:** November 2025 | **License:** See repository

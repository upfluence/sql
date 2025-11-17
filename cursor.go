package sql

// ScanFunc is a function that processes a single row from a query result.
type ScanFunc func(Scanner) error

// Cursor is an iterator over query result rows.
// It is similar to database/sql Rows but with a more idiomatic Go API.
type Cursor interface {
	// Scanner provides access to row values.
	Scanner

	// Next advances the cursor to the next row.
	// It returns false when there are no more rows or an error occurred.
	Next() bool

	// Close releases resources associated with the cursor.
	// Subsequent calls to Scan, Next, or Err will fail.
	Close() error

	// Err returns the first error encountered during iteration, if any.
	Err() error
}

// ScrollCursor iterates over the rows in c, calling fn for each row.
// It closes c before returning.
//
//	err := sql.ScrollCursor(rows, func(scanner sql.Scanner) error {
//		var id int
//		var name string
//		if err := scanner.Scan(&id, &name); err != nil {
//			return err
//		}
//		fmt.Printf("User %d: %s\n", id, name)
//		return nil
//	})
func ScrollCursor(c Cursor, fn ScanFunc) error {
	defer c.Close()

	for c.Next() {
		if err := fn(c); err != nil {
			return err
		}
	}

	return c.Err()
}

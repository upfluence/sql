package sql

// ConstraintType is the kind of database constraint that was violated.
type ConstraintType int

const (
	// PrimaryKey is a primary key constraint violation.
	PrimaryKey ConstraintType = iota + 1

	// ForeignKey is a foreign key constraint violation.
	ForeignKey

	// NotNull is a NOT NULL constraint violation.
	NotNull

	// Unique is a unique constraint violation.
	Unique
)

// ConstraintError is returned when a database constraint is violated.
// The Type field identifies the kind of constraint, and Constraint contains
// the constraint name if available.
type ConstraintError struct {
	Type       ConstraintType
	Constraint string
	Cause      error
}

func (ce ConstraintError) Error() string { return ce.Cause.Error() }

// RollbackType is the reason a transaction was rolled back.
type RollbackType int

const (
	// SerializationFailure indicates a transaction conflict due to concurrent modifications.
	// This error is retryable.
	SerializationFailure RollbackType = iota + 1

	// Locked indicates a transaction failed because a required resource was locked.
	// This error is retryable.
	Locked
)

// RollbackError is returned when a transaction is rolled back due to a transient error.
// ExecuteTx automatically retries transactions that return RollbackError.
type RollbackError struct {
	Type  RollbackType
	Cause error
}

func (re RollbackError) Error() string { return re.Cause.Error() }

package sql

type ConstraintType int

const (
	PrimaryKey ConstraintType = iota + 1
	ForeignKey
	NotNull
	Unique
)

type ConstraintError struct {
	Type       ConstraintType
	Constraint string

	Cause error
}

func (ce ConstraintError) Error() string {
	return ce.Cause.Error()
}

type RollbackType int

const (
	SerializationFailure RollbackType = iota + 1
	Locked
)

type RollbackError struct {
	Type  RollbackType
	Cause error
}

func (re RollbackError) Error() string {
	return re.Cause.Error()
}

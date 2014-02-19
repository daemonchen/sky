package db

var (
	// ErrDatabaseOpen is returned when the database is already open.
	ErrDatabaseOpen = &Error{"database already open", nil}

	// ErrDatabaseNotOpen is returned when an operation is being attempted but
	// the database is not in an open state.
	ErrDatabaseNotOpen = &Error{"database not open", nil}
)

var (
	// ErrTableOpen is returned when open a table that is already open.
	ErrTableOpen = &Error{"table already open", nil}

	// ErrTableNotOpen is returned when operating on a closed table.
	ErrTableNotOpen = &Error{"table not open", nil}

	// ErrTableExists is returned when creating a table that already exists.
	ErrTableExists = &Error{"table already exists", nil}

	// ErrTableNotFound is returned when accessing a table that doesn't exist.
	ErrTableNotFound = &Error{"table does not exist", nil}

	// ErrTableNameRequired is returned when open a table without a name.
	ErrTableNameRequired = &Error{"table name required", nil}
)

var (
	// ErrInvalidPropertyName is returned when creating a property with a name
	// containing non-alphanumeric characters.
	ErrInvalidPropertyName = &Error{"invalid property name", nil}

	// ErrPropertyNotFound is returned when a property cannot be found.
	ErrPropertyNotFound = &Error{"property not found", nil}

	// ErrPropertyExists is returned when creating a property that
	// already exists.
	ErrPropertyExists = &Error{"property already exits", nil}
)

var (
	// ErrInvalidDataType is returned when creating a property with an
	// invalid data type.
	ErrInvalidDataType = &Error{"invalid data type", nil}
)

// Error represents an error condition caused by the database.
type Error struct {
	message string
	cause   error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return e.message + ": " + e.cause.Error()
	}
	return e.message
}

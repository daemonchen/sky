package db

import "errors"

var (
	// ErrDatabaseOpen is returned when the database is already open.
	ErrDatabaseOpen = errors.New("database already open")

	// ErrDatabaseNotOpen is returned when an operation is being attempted but
	// the database is not in an open state.
	ErrDatabaseNotOpen = errors.New("database not open")

	// ErrTableOpen is returned when open a table that is already open.
	ErrTableOpen = errors.New("table already open")

	// ErrTableNotOpen is returned when operating on a closed table.
	ErrTableNotOpen = errors.New("table not open")

	// ErrTableExists is returned when creating a table that already exists.
	ErrTableExists = errors.New("table already exists")

	// ErrTableNotFound is returned when accessing a table that doesn't exist.
	ErrTableNotFound = errors.New("table not found")

	// ErrTableNameRequired is returned when open a table without a name.
	ErrTableNameRequired = errors.New("table name required")

	// ErrTableMetaError is returned when there is a problem reading the
	// table meta data.
	ErrTableMetaError = errors.New("table meta error")

	// ErrObjectIDRequired is returned inserting, deleting, or retrieving
	// event data without specifying an object identifier.
	ErrObjectIDRequired = errors.New("object id required")

	// ErrFactorNotFound is returned when defactorizing a value that has not
	// previously been factorized.
	ErrFactorNotFound = errors.New("factor not found")

	// ErrInvalidPropertyName is returned when creating a property with a name
	// containing non-alphanumeric characters.
	ErrInvalidPropertyName = errors.New("invalid property name")

	// ErrPropertyNotFound is returned when a property cannot be found.
	ErrPropertyNotFound = errors.New("property not found")

	// ErrPropertyExists is returned when creating a property that
	// already exists.
	ErrPropertyExists = errors.New("property already exits")

	// ErrInvalidDataType is returned when creating a property with an
	// invalid data type.
	ErrInvalidDataType = errors.New("invalid data type")
)

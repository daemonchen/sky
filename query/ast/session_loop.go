package ast

import (
	"fmt"
)

// SessionLoop represents a statement that iterates over individual sessions.
type SessionLoop struct {
	Statements   Statements
	IdleDuration int
}

func (l *SessionLoop) node()      {}
func (l *SessionLoop) block()     {}
func (l *SessionLoop) statement() {}

// NewSessionLoop creates a new SessionLoop instance.
func NewSessionLoop() *SessionLoop {
	return &SessionLoop{}
}

func (l *SessionLoop) ClauseString() string {
	quantity, units := SecondsToTimeSpan(l.IdleDuration)
	return fmt.Sprintf("FOR EACH SESSION DELIMITED BY %d %s", quantity, units)
}

func (l *SessionLoop) String() string {
	str := l.ClauseString() + "\n"
	str += lineStartRegex.ReplaceAllString(l.Statements.String(), "  ") + "\n"
	str += "END"
	return str
}

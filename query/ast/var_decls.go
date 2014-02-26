package ast

// A slice of VarDecl objects. Declarations sort by system, id, name.
type VarDecls []*VarDecl

func (s VarDecls) Len() int {
	return len(s)
}

func (s VarDecls) Less(i, j int) bool {
	// System variables go first.
	isys, jsys := s[i].IsSystem(), s[j].IsSystem()
	if isys != jsys {
		return isys
	}

	// Then sort by id.
	if s[i].ID != s[j].ID {
		return s[i].ID < s[j].ID
	}

	// Finally sort by name.
	return s[i].Name < s[j].Name
}

func (s VarDecls) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

package cmd

import "fmt"

const (
	// MajorVersion is the major version.
	MajorVersion = 0

	// MinorVersion is the minor version.
	MinorVersion = 4

	// PatchVersion is the patch level.
	PatchVersion = 0
)

// Version returns the full version string.
func Version() string {
	return fmt.Sprintf("v%d.%d.%d", MajorVersion, MinorVersion, PatchVersion)
}

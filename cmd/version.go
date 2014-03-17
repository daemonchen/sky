package version

import "fmt"

const (
	branchMarker = ""
	commitMarker = ""
)

const (
	// Major is the major version.
	Major = 0

	// Minor is the minor version.
	Minor = 4

	// Patch is the patch level.
	Patch = 0

	// Branch is the branch that Sky was built from.
	Branch = "llvm" + branchMarker

	// Commit is the short SHA1 git commit Sky was built from.
	Commit = "" + commitMarker
)

// String returns the full version string.
func String() string {
	version := fmt.Sprintf("v%d.%d.%d", Major, Minor, Patch)
	if Branch != "" && Commit != "" {
		return fmt.Sprintf("%s (%s/%s)", version, Branch, Commit)
	}
	return version
}

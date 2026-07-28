// Package migrations embeds the service's golang-migrate SQL files so they can
// be applied at startup via platform-pgcommon's migrate.Runner without shipping
// the .sql files alongside the binary.
package migrations

import "embed"

// FS holds the embedded up/down migration files. It is passed as the migrate.Runner.FS.
//
//go:embed *.sql
var FS embed.FS

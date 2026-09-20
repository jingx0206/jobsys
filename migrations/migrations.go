// Package migrations embeds the SQL schema so the api binary can apply it with
// `api migrate`, which is how Compose and Kubernetes set up the database.
package migrations

import "embed"

// FS holds the top-level *.sql files, applied in file name order. The checks
// directory is not embedded.
//
//go:embed *.sql
var FS embed.FS

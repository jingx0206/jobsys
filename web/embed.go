// Package web holds the browser UI, which the API serves at /.
package web

import "embed"

// FS holds the page and its script and styles.
//
//go:embed index.html app.js style.css
var FS embed.FS

// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.16.0

package query

import (
	"time"
)

type File struct {
	Namespace string
	Enabled   bool
	Partition string
	Name      string
	Bytes     int64
	Rows      int64
	Columns   []string
	CreatedAt time.Time
	UpdatedAt time.Time
}

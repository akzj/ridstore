// Package ridstore provides an embedded stable-ID log-structured record store.
package ridstore

import "github.com/akzj/ridstore/internal/base"

// ID is a stable logical record identifier. Zero is invalid.
type ID = base.ID

// BatchID uniquely identifies a batch. Zero is invalid.
type BatchID = base.BatchID

// CommitSeq orders durable user commits and internal relocations.
type CommitSeq = base.CommitSeq

// Revision is an opaque logical record revision.
type Revision = base.Revision

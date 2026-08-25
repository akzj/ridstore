package model

// ID is a stable logical record identity. Zero is reserved.
type ID uint64

// BatchID identifies one user or internal atomic mutation batch. Zero is reserved.
type BatchID uint64

// CommitSeq is the logical publication order. Zero is reserved.
type CommitSeq uint64

// Revision is the logical version observed by optimistic conditions. User
// writes use their BatchID value as Revision; relocation preserves it.
type Revision uint64

type MapSegmentID uint32

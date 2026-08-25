package model

// ID is a stable logical record identity. Zero is reserved.
type ID uint64

// BatchID identifies one user or internal atomic mutation batch. Zero is reserved.
type BatchID uint64

// CommitSeq is the logical publication order. Zero is reserved.
type CommitSeq uint64

// MapAddr identifies a node in the persistent mapping store. Zero means no node.
type MapAddr uint64

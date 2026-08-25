package recordcodec

import "testing"

func FuzzDecodePut(f *testing.F) {
	seed, _ := EncodePut(PutRecord{OriginBatchID: 1, RecordID: 2, Value: []byte("value")}, 1024)
	f.Add(seed)
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _ = DecodePut(value, 1<<20)
	})
}

func FuzzDecodeCommitGroup(f *testing.F) {
	seed, _ := EncodeCommitGroup(CommitGroup{Descriptors: []Descriptor{{Kind: DescriptorUserCommit, BatchID: 1, CommitSeq: 1}}}, 1024)
	f.Add(seed)
	f.Fuzz(func(t *testing.T, value []byte) {
		_, _ = DecodeCommitGroup(value, 1<<20, 1024, 1<<16)
	})
}

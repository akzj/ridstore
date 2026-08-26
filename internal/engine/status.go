package engine

import "math"

// addStatusLocked retains the newest terminal outcomes. Admission reserves one
// slot per open user Batch, including every Batch that can become unknown when
// the Store fails closed.
func (s *Store) addStatusLocked(status BatchStatus) {
	s.statusSerial++
	entry := statusEntry{status: status, serial: s.statusSerial}
	s.statuses[status.BatchID] = entry
	s.statusOrder = append(s.statusOrder, statusOrderEntry{id: status.BatchID, serial: entry.serial})
	for uint64(len(s.statuses)) > s.statusRetention && s.statusOrderHead < len(s.statusOrder) {
		oldest := s.statusOrder[s.statusOrderHead]
		s.statusOrderHead++
		current, exists := s.statuses[oldest.id]
		if !exists || current.serial != oldest.serial {
			continue
		}
		delete(s.statuses, oldest.id)
		s.recoveryAbortedValid = false
	}
	if uint64(s.statusOrderHead) > s.statusRetention && s.statusOrderHead*2 >= len(s.statusOrder) {
		copy(s.statusOrder, s.statusOrder[s.statusOrderHead:])
		s.statusOrder = s.statusOrder[:len(s.statusOrder)-s.statusOrderHead]
		s.statusOrderHead = 0
	}
}

func (s *Store) statusCapacityAvailableLocked() bool {
	if s.terminalTotal < s.terminalBase {
		return false
	}
	tail := s.terminalTotal - s.terminalBase
	if tail > math.MaxUint64-uint64(s.openCount) {
		return false
	}
	return tail+uint64(s.openCount) < s.statusRetention
}

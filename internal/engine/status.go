package engine

import "math"

// addStatusLocked retains the newest terminal outcomes. Admission reserves one
// slot per open user Batch, including every Batch that can become unknown when
// the Store fails closed.
func (s *Store) addStatusLocked(status BatchStatus) {
	s.state.statusSerial++
	entry := statusEntry{status: status, serial: s.state.statusSerial}
	s.state.statuses[status.BatchID] = entry
	s.state.statusOrder = append(s.state.statusOrder, statusOrderEntry{id: status.BatchID, serial: entry.serial})
	for uint64(len(s.state.statuses)) > s.state.statusRetention && s.state.statusOrderHead < len(s.state.statusOrder) {
		oldest := s.state.statusOrder[s.state.statusOrderHead]
		s.state.statusOrderHead++
		current, exists := s.state.statuses[oldest.id]
		if !exists || current.serial != oldest.serial {
			continue
		}
		delete(s.state.statuses, oldest.id)
		s.state.recoveryAbortedValid = false
	}
	if uint64(s.state.statusOrderHead) > s.state.statusRetention && s.state.statusOrderHead*2 >= len(s.state.statusOrder) {
		copy(s.state.statusOrder, s.state.statusOrder[s.state.statusOrderHead:])
		s.state.statusOrder = s.state.statusOrder[:len(s.state.statusOrder)-s.state.statusOrderHead]
		s.state.statusOrderHead = 0
	}
}

func (s *Store) statusCapacityAvailableLocked() bool {
	if s.state.terminalTotal < s.state.terminalBase {
		return false
	}
	tail := s.state.terminalTotal - s.state.terminalBase
	if tail > math.MaxUint64-uint64(s.state.openCount) {
		return false
	}
	return tail+uint64(s.state.openCount) < s.state.statusRetention
}

package engine

import (
	"math"
	"sync"
	"time"

	"github.com/akzj/ridstore/internal/model"
	"github.com/akzj/ridstore/internal/recordlog"
	"github.com/akzj/ridstore/internal/storecatalog"
)

const gcHistoryLimit = 8

type gcSample struct {
	covered   model.CommitSeq
	at        time.Time
	liveBytes uint64
}

type gcSegmentHistory struct {
	firstSeen time.Time
	samples   []gcSample
}

type gcStability struct {
	mu       sync.Mutex
	segments map[recordlog.SegmentID]gcSegmentHistory
}

type gcStabilityView struct {
	Age                    time.Duration
	StableRounds           uint32
	LatestDeathBytes       uint64
	LatestDeathPerCommit   uint64
	LatestDeathBytesPerSec uint64
}

func (g *gcStability) sample(manifest storecatalog.Manifest, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.segments == nil {
		g.segments = make(map[recordlog.SegmentID]gcSegmentHistory)
	}
	live := make(map[recordlog.SegmentID]uint64, len(manifest.SegmentStats))
	for _, stat := range manifest.SegmentStats {
		live[stat.SegmentID] = stat.LiveBytes
	}
	seen := make(map[recordlog.SegmentID]struct{}, len(manifest.SealedDataSegments))
	for _, segment := range manifest.SealedDataSegments {
		seen[segment.SegmentID] = struct{}{}
		history := g.segments[segment.SegmentID]
		if history.firstSeen.IsZero() {
			history.firstSeen = now
		}
		sample := gcSample{covered: manifest.StatsCoveredCommitSeq, at: now, liveBytes: live[segment.SegmentID]}
		if len(history.samples) == 0 || history.samples[len(history.samples)-1] != sample {
			history.samples = append(history.samples, sample)
			if len(history.samples) > gcHistoryLimit {
				history.samples = append([]gcSample(nil), history.samples[len(history.samples)-gcHistoryLimit:]...)
			}
		}
		g.segments[segment.SegmentID] = history
	}
	for id := range g.segments {
		if _, ok := seen[id]; !ok {
			delete(g.segments, id)
		}
	}
}

func (g *gcStability) view(id recordlog.SegmentID, now time.Time, policy CompactionPolicy) (gcStabilityView, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	history, ok := g.segments[id]
	if !ok || len(history.samples) == 0 {
		return gcStabilityView{}, false
	}
	view := gcStabilityView{Age: now.Sub(history.firstSeen)}
	for index := len(history.samples) - 1; index > 0; index-- {
		current, previous := history.samples[index], history.samples[index-1]
		if current.liveBytes > previous.liveBytes {
			break
		}
		death := previous.liveBytes - current.liveBytes
		commitDelta := uint64(current.covered - previous.covered)
		perCommit := death
		if commitDelta != 0 {
			perCommit = death / commitDelta
			if death%commitDelta != 0 {
				perCommit++
			}
		}
		seconds := current.at.Sub(previous.at).Seconds()
		perSecond := uint64(0)
		if seconds > 0 && death != 0 {
			if float64(death)/seconds >= float64(math.MaxUint64) {
				perSecond = math.MaxUint64
			} else {
				perSecond = uint64(math.Ceil(float64(death) / seconds))
			}
		}
		if index == len(history.samples)-1 {
			view.LatestDeathBytes = death
			view.LatestDeathPerCommit = perCommit
			view.LatestDeathBytesPerSec = perSecond
		}
		if perCommit > policy.MaxDeathBytesPerCommit || perSecond > policy.MaxDeathBytesPerSecond {
			break
		}
		view.StableRounds++
	}
	return view, true
}

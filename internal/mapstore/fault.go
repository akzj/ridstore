package mapstore

type FaultPoint string

const (
	FaultBeforeAppendWrite  FaultPoint = "mapstore.before-append-write"
	FaultBeforeSync         FaultPoint = "mapstore.before-sync"
	FaultBeforeTailTruncate FaultPoint = "mapstore.before-tail-truncate"
	FaultBeforeTailSync     FaultPoint = "mapstore.before-tail-sync"
)

type FaultHook func(FaultPoint) error

func hitFault(hook FaultHook, point FaultPoint) error {
	if hook == nil {
		return nil
	}
	return hook(point)
}

package mapstore

type FaultPoint string

const (
	FaultBeforeAppendWrite    FaultPoint = "mapstore.before-append-write"
	FaultBeforeSync           FaultPoint = "mapstore.before-sync"
	FaultBeforeTailTruncate   FaultPoint = "mapstore.before-tail-truncate"
	FaultBeforeTailSync       FaultPoint = "mapstore.before-tail-sync"
	FaultBeforeJournalWrite   FaultPoint = "mapstore.rotation.before-journal-write"
	FaultBeforeJournalSync    FaultPoint = "mapstore.rotation.before-journal-sync"
	FaultBeforeJournalRename  FaultPoint = "mapstore.rotation.before-journal-rename"
	FaultBeforeJournalDirSync FaultPoint = "mapstore.rotation.before-journal-dir-sync"
	FaultBeforeFooterWrite    FaultPoint = "mapstore.rotation.before-footer-write"
	FaultBeforeFooterSync     FaultPoint = "mapstore.rotation.before-footer-sync"
	FaultBeforeSealRename     FaultPoint = "mapstore.rotation.before-seal-rename"
	FaultBeforeSealDirSync    FaultPoint = "mapstore.rotation.before-seal-dir-sync"
	FaultBeforeHeaderWrite    FaultPoint = "mapstore.rotation.before-header-write"
	FaultBeforeHeaderSync     FaultPoint = "mapstore.rotation.before-header-sync"
	FaultBeforeCreateRename   FaultPoint = "mapstore.rotation.before-create-rename"
	FaultBeforeCreateDirSync  FaultPoint = "mapstore.rotation.before-create-dir-sync"
	FaultBeforeJournalRemove  FaultPoint = "mapstore.rotation.before-journal-remove"
	FaultBeforeCleanupDirSync FaultPoint = "mapstore.rotation.before-cleanup-dir-sync"
)

type FaultHook func(FaultPoint) error

func hitFault(hook FaultHook, point FaultPoint) error {
	if hook == nil {
		return nil
	}
	return hook(point)
}

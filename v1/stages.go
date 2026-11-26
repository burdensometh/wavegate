package v1

const (
	Waiting     = "Waiting"
	Progressing = "Progressing"
	Paused      = "Paused"
	RollingBack = "RollingBack"
	Aborted     = "Aborted"
	Promoted    = "Promoted"

	Initialize    = "Initialize"
	WeightStep    = "WeightStep"
	RefreshStep   = "RefreshStep"
	RefreshPaused = "RefreshPaused"
	RollBack      = "RollBack"
	Promote       = "Promote"
	SyncStable    = "SyncStable"
)

func Terminal(phase string) bool { return phase == Aborted || phase == Promoted }

func ChangesCandidate(kind string) bool {
	return kind == WeightStep || kind == RefreshStep || kind == RefreshPaused || kind == Promote
}

func KnownPhase(phase string) bool {
	switch phase {
	case Waiting, Progressing, Paused, RollingBack, Aborted, Promoted:
		return true
	}
	return false
}

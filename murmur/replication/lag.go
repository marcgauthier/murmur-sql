package replication

// lagBehind computes a peer's replication lag against local per-origin
// heads: the worst per-origin applied deficit, and how many origins lag
// at all. Origins where the peer is ahead of (or level with) the local
// head contribute nothing; silence counts as fully behind.
func lagBehind(heads []Progress, applied map[NodeId]uint64) (worst uint64, behind int) {
	for _, h := range heads {
		if l := satSub(h.Applied, applied[h.Origin]); l > 0 {
			behind++
			if l > worst {
				worst = l
			}
		}
	}
	return worst, behind
}

// observedDeficit computes local staleness against peers' observed heads:
// the worst deficit of local applied heads behind anything any peer saw.
// Origins unknown locally count as nothing applied. No peers means no
// known-newer state, hence zero.
func observedDeficit(heads []Progress, peers []map[NodeId]uint64) uint64 {
	applied := make(map[NodeId]uint64, len(heads))
	for _, h := range heads {
		applied[h.Origin] = h.Applied
	}
	var worst uint64
	for _, obs := range peers {
		for origin, seen := range obs {
			if d := satSub(seen, applied[origin]); d > worst {
				worst = d
			}
		}
	}
	return worst
}

// memberStateName renders a membership state for peer diagnostics.
// Members the view has never seen report "unknown".
func memberStateName(state MemberState, known bool) string {
	if !known {
		return "unknown"
	}
	switch state {
	case MemberAlive:
		return "alive"
	case MemberSuspect:
		return "suspect"
	case MemberDead:
		return "dead"
	case MemberLeft:
		return "left"
	default:
		return "unknown"
	}
}

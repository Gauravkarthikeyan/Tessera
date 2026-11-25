package cluster

import "testing"

// TestDuplicateOfAcceptedRequestIsNotRejected pins the bug seed 108 found.
//
// The network duplicated a StoreReq. The first copy passed the partition's
// token bucket and was proposed; the duplicate arrived a moment later, found
// the bucket empty and was answered "throttled" — a definite rejection — which
// reached the router before the commit did. The client recorded the put as
// never having happened, a later put came back with version 6, and the
// checker (correctly) found no history in which version 5 existed.
//
// Seed 108 is run with the configuration it originally failed under.
func TestDuplicateOfAcceptedRequestIsNotRejected(t *testing.T) {
	for _, seed := range []int64{108, 112, 130} {
		cfg := DefaultConfig(seed)
		cfg.PreVote = false
		if rep := New(cfg).Run(); !rep.Check.OK {
			t.Errorf("seed %d:\n%s", seed, rep.Summary(true))
		}
	}
}

// TestPreVoteReducesChurn checks the reason pre-vote is on by default: on the
// same seeds, far fewer elections and fewer live leaders deposed.
func TestPreVoteReducesChurn(t *testing.T) {
	var off, on Stats
	for seed := int64(1); seed <= 5; seed++ {
		for _, pv := range []bool{false, true} {
			cfg := DefaultConfig(seed)
			cfg.PreVote = pv
			rep := New(cfg).Run()
			if !rep.Check.OK {
				t.Fatalf("seed %d prevote=%v not linearizable", seed, pv)
			}
			s := &off
			if pv {
				s = &on
			}
			s.Elections += rep.Stats.Elections
			s.LeaderLosses += rep.Stats.LeaderLosses
		}
	}
	t.Logf("elections: %d → %d, leaders deposed while alive: %d → %d", off.Elections, on.Elections, off.LeaderLosses, on.LeaderLosses)
	if on.Elections*2 > off.Elections {
		t.Errorf("pre-vote should at least halve elections: %d → %d", off.Elections, on.Elections)
	}
	if on.LeaderLosses >= off.LeaderLosses {
		t.Errorf("pre-vote should depose fewer live leaders: %d → %d", off.LeaderLosses, on.LeaderLosses)
	}
}

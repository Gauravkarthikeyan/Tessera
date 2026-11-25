package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/Gauravkarthikeyan/tessera/internal/sim"
)

// peer hosts one Raft instance on a simulated node and records everything it
// applies, so the test can check Raft's safety properties after the run.
type peer struct {
	w       *sim.World
	id      sim.NodeID
	r       *Raft
	applied *[]Entry // durable across crashes only for inspection
}

type world struct {
	w       *sim.World
	ids     []sim.NodeID
	applied map[sim.NodeID]*[]Entry
	leaders map[uint64]sim.NodeID // term → leader (election safety)
	t       *testing.T
	unsafe  bool
	preVote bool
}

func (p *peer) Receive(_ sim.NodeID, msg any) {
	p.r.Step(msg.(Message))
	p.flush()
}

func (p *peer) flush() {
	for _, m := range p.r.Messages() {
		p.w.Send(p.id, m.To, m)
	}
	for _, e := range p.r.Committed() {
		*p.applied = append(*p.applied, e)
	}
}

func newWorld(t *testing.T, seed int64, n int, unsafe, preVote bool) *world {
	ww := &world{
		w:       sim.NewWorld(seed, sim.DefaultFaults),
		applied: map[sim.NodeID]*[]Entry{},
		leaders: map[uint64]sim.NodeID{},
		t:       t,
		unsafe:  unsafe,
		preVote: preVote,
	}
	for i := 1; i <= n; i++ {
		ww.ids = append(ww.ids, sim.NodeID(i))
	}
	for _, id := range ww.ids {
		ww.w.Spawn(id, ww.factory)
	}
	return ww
}

func (ww *world) factory(w *sim.World, id sim.NodeID, disk *sim.Disk) sim.Node {
	hs, _ := disk.Get("hs").(*HardState)
	if hs == nil {
		hs = NewHardState()
		disk.Put("hs", hs)
	}
	// Volatile: a restarted node re-applies the log from the beginning.
	app := &[]Entry{}
	ww.applied[id] = app
	p := &peer{w: w, id: id, applied: app}
	p.r = New(Config{
		ID: id, Peers: ww.ids, ElectionTicks: 10, HeartbeatTicks: 2,
		Rand:                  rand.New(rand.NewSource(w.Rand().Int63())),
		UnsafeCommitPriorTerm: ww.unsafe,
		PreVote:               ww.preVote,
	}, hs)
	var tick func()
	tick = func() {
		p.r.Tick()
		p.flush()
		ww.observe(p)
		w.After(id, 10*time.Millisecond, tick)
	}
	w.After(id, 10*time.Millisecond, tick)
	return p
}

func (ww *world) observe(p *peer) {
	if p.r.IsLeader() {
		if prev, ok := ww.leaders[p.r.Term()]; ok && prev != p.id {
			ww.t.Fatalf("election safety violated: n%d and n%d both leader in term %d", prev, p.id, p.r.Term())
		}
		ww.leaders[p.r.Term()] = p.id
	}
}

func (ww *world) propose(v int) {
	for _, id := range ww.ids {
		if n, ok := ww.w.Node(id).(*peer); ok && n.r.IsLeader() {
			n.r.Propose(v)
			n.flush()
			return
		}
	}
}

// checkStateMachineSafety: every node applies the same entry at each index.
func (ww *world) checkStateMachineSafety() int {
	ref := map[uint64]Entry{}
	maxLen := 0
	for id, app := range ww.applied {
		for i, e := range *app {
			if e.Index != uint64(i+1) {
				ww.t.Fatalf("n%d applied out of order: position %d has index %d", id, i, e.Index)
			}
			if r, ok := ref[e.Index]; ok {
				if r.Term != e.Term || fmt.Sprint(r.Data) != fmt.Sprint(e.Data) {
					ww.t.Fatalf("state machine safety violated at index %d: %v vs %v", e.Index, r, e)
				}
			} else {
				ref[e.Index] = e
			}
		}
		maxLen = max(maxLen, len(*app))
	}
	return maxLen
}

func runChaos(t *testing.T, seed int64, unsafe, preVote bool) (applied int) {
	ww := newWorld(t, seed, 5, unsafe, preVote)
	rng := rand.New(rand.NewSource(seed))
	v := 0
	for step := 0; step < 400; step++ {
		ww.w.RunFor(25 * time.Millisecond)
		for i := 0; i < 3; i++ {
			v++
			ww.propose(v)
		}
		switch x := rng.Intn(100); {
		case x < 4:
			ww.w.Crash(ww.ids[rng.Intn(len(ww.ids))])
		case x < 10:
			for _, id := range ww.ids {
				ww.w.Restart(id)
			}
		case x < 13:
			perm := rng.Perm(len(ww.ids))
			var a, b []sim.NodeID
			for i, j := range perm {
				if i < 2 {
					a = append(a, ww.ids[j])
				} else {
					b = append(b, ww.ids[j])
				}
			}
			ww.w.Partition(a, b)
		case x < 18:
			ww.w.Heal()
		}
		ww.checkStateMachineSafety()
	}
	ww.w.Heal()
	for _, id := range ww.ids {
		ww.w.Restart(id)
	}
	ww.w.RunFor(3 * time.Second)
	ww.propose(-1)
	ww.w.RunFor(1 * time.Second)
	return ww.checkStateMachineSafety()
}

func TestRaftSafetyUnderChaos(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		for seed := int64(1); seed <= 40; seed++ {
			t.Run(fmt.Sprintf("prevote=%v/seed%d", preVote, seed), func(t *testing.T) {
				if n := runChaos(t, seed, false, preVote); n == 0 {
					t.Fatalf("no progress")
				}
			})
		}
	}
}

// net is a tiny hand-driven network for scenario tests: messages are
// delivered in FIFO order, and any node in cut is unreachable both ways.
type net struct {
	nodes map[NodeID]*Raft
	cut   map[NodeID]bool
}

func newNet(n int, preVote bool) *net {
	nt := &net{nodes: map[NodeID]*Raft{}, cut: map[NodeID]bool{}}
	var peers []NodeID
	for i := 1; i <= n; i++ {
		peers = append(peers, NodeID(i))
	}
	for _, id := range peers {
		nt.nodes[id] = New(Config{
			ID: id, Peers: peers, ElectionTicks: 10, HeartbeatTicks: 1,
			Rand: rand.New(rand.NewSource(int64(id))), PreVote: preVote,
		}, NewHardState())
	}
	return nt
}

func (nt *net) settle() {
	for {
		var q []Message
		for i := NodeID(1); int(i) <= len(nt.nodes); i++ {
			q = append(q, nt.nodes[i].Messages()...)
		}
		if len(q) == 0 {
			return
		}
		for _, m := range q {
			if !nt.cut[m.From] && !nt.cut[m.To] {
				nt.nodes[m.To].Step(m)
			}
		}
	}
}

func (nt *net) tick(id NodeID) { nt.nodes[id].Tick(); nt.settle() }

func (nt *net) tickAll(n int) {
	for ; n > 0; n-- {
		for i := NodeID(1); int(i) <= len(nt.nodes); i++ {
			nt.tick(i)
		}
	}
}

func (nt *net) leader() NodeID {
	for i := NodeID(1); int(i) <= len(nt.nodes); i++ {
		if nt.nodes[i].IsLeader() {
			return i
		}
	}
	return None
}

// TestPreVoteRejoiningNodeDoesNotDisrupt is the scenario pre-vote exists
// for: a follower is cut off, times out over and over, and then the
// partition heals. Without pre-vote its term has run far ahead and the
// first message it sends forces the healthy leader to step down. With
// pre-vote its term never moved, and the cluster doesn't notice it was gone.
func TestPreVoteRejoiningNodeDoesNotDisrupt(t *testing.T) {
	for _, preVote := range []bool{false, true} {
		t.Run(fmt.Sprintf("prevote=%v", preVote), func(t *testing.T) {
			nt := newNet(3, preVote)
			nt.tick(1)
			for i := 0; i < 30; i++ {
				nt.tick(1) // only n1 ticks, so n1 wins the first election
			}
			lead := nt.leader()
			if lead != 1 {
				t.Fatalf("expected n1 to lead, got n%d", lead)
			}
			term := nt.nodes[1].Term()

			var isolated NodeID = 3
			nt.cut[isolated] = true
			nt.tickAll(200) // ~10 election timeouts on the isolated node
			gotTerm := nt.nodes[isolated].Term()
			if preVote && gotTerm != term {
				t.Fatalf("isolated node bumped its term %d → %d despite pre-vote", term, gotTerm)
			}
			if !preVote && gotTerm <= term {
				t.Fatalf("expected the isolated node's term to run ahead without pre-vote")
			}

			delete(nt.cut, isolated)
			nt.tickAll(3)
			disrupted := nt.nodes[1].Term() != term
			if preVote && disrupted {
				t.Fatalf("leader n1 was deposed (term %d → %d) by a rejoining node", term, nt.nodes[1].Term())
			}
			if !preVote && !disrupted {
				t.Fatalf("expected the rejoining node to force a new term without pre-vote")
			}
			for i := 0; i < 100 && nt.leader() == None; i++ {
				nt.tickAll(1)
			}
			if nt.leader() == None {
				t.Fatalf("no leader after heal")
			}
		})
	}
}

// TestPreVoteStillElectsAfterLeaderDies: stickiness must not get in the way
// of replacing a leader that is actually gone.
func TestPreVoteStillElectsAfterLeaderDies(t *testing.T) {
	nt := newNet(3, true)
	for i := 0; i < 30; i++ {
		nt.tick(1)
	}
	if nt.leader() != 1 {
		t.Fatal("n1 did not become leader")
	}
	nt.cut[1] = true
	for i := 0; i < 100 && nt.nodes[2].Role() != Leader && nt.nodes[3].Role() != Leader; i++ {
		nt.tick(2)
		nt.tick(3)
	}
	if !nt.nodes[2].IsLeader() && !nt.nodes[3].IsLeader() {
		t.Fatal("no new leader elected after the old one was cut off")
	}
}

func TestSingleNodeCommitsImmediately(t *testing.T) {
	hs := NewHardState()
	r := New(Config{ID: 1, Peers: []NodeID{1}, Rand: rand.New(rand.NewSource(1))}, hs)
	for i := 0; i < 30 && !r.IsLeader(); i++ {
		r.Tick()
	}
	if !r.IsLeader() {
		t.Fatal("single node did not elect itself")
	}
	r.Propose("x")
	got := r.Committed()
	if len(got) != 2 || got[1].Data != "x" {
		t.Fatalf("got %v", got)
	}
}

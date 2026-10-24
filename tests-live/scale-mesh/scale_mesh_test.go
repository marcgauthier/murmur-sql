// Live scenario: a 10-node gossip mesh with fanout 4 converges 400
// rows while no node ever pushes to more than 4 peers.
//
// Go port of tests/scale_mesh.rs using tests-live/goharness. Identical
// behavior: ten real servers mesh with fanout 4, a sampler asserts every
// 100 ms that each node's selected push peers (connected and selected)
// never exceed the fanout, 300 rows land on node 0 and 100 on node 7,
// all ten nodes converge within 300 s, and the sampler took at least one
// full pass. The Rust tokio sampler becomes a goroutine reporting over
// a channel.
package scalemesh

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	goharness "github.com/marcgauthier/murmur-sql/tests-live/goharness"
)

func TestScaleMesh(t *testing.T) {
	if os.Getenv("MURMUR_ENGINE_INTEGRATION") != "1" {
		t.Skip("live scenarios are opt-in (set MURMUR_ENGINE_INTEGRATION=1)")
	}
	fanout := 4
	opts := goharness.DefaultClusterOptions("scale-mesh-sql")
	opts.NumNodes = 10
	opts.Replication = &goharness.ReplicationOptions{Fanout: &fanout}
	cluster, err := goharness.NewCluster(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	failed := true
	defer func() { cluster.Finish(failed) }()

	if err := goharness.Fixture(cluster); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	var stop atomic.Bool
	type sampleOut struct {
		samples int
		err     error
	}
	done := make(chan sampleOut, 1)
	go func() {
		samples := 0
		for {
			for i := 0; i < 10; i++ {
				peers, err := goharness.Peers(cluster.Node(i))
				if err != nil {
					done <- sampleOut{err: err}
					return
				}
				selected := 0
				for _, p := range peers {
					if len(p) > 6 && p[2] == "true" && p[6] == "true" {
						selected++
					}
				}
				if selected > 4 {
					done <- sampleOut{err: fmt.Errorf("node %d: push peers %d>fanout 4", i, selected)}
					return
				}
				samples++
			}
			if stop.Load() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		done <- sampleOut{samples: samples}
	}()
	var expected [][]string
	for n := 0; n < 400; n++ {
		node := 0
		if n >= 300 {
			node = 7
		}
		id := fmt.Sprintf("%06d", n)
		name := fmt.Sprintf("origin-%d-%d", node, n)
		if _, err := cluster.Node(node).Execute(
			"INSERT INTO records(id,name) VALUES($1,$2)", id, name); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
		expected = append(expected, []string{id, name})
	}
	for _, node := range []int{0, 7} {
		if _, err := cluster.Node(node).Simple("MURMUR FLUSH"); err != nil {
			t.Fatalf("node %d flush: %v", node, err)
		}
	}
	all := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	if err := goharness.Converge(cluster, all, expected, 300*time.Second); err != nil {
		t.Fatalf("converge: %v", err)
	}
	stop.Store(true)
	out := <-done
	if out.err != nil {
		t.Fatalf("sampler: %v", out.err)
	}
	if out.samples < 10 {
		t.Fatalf("sampler took %d samples, want >= 10", out.samples)
	}
	failed = false
}

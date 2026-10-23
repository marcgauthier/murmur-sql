package murmur

import (
	"math"
	"math/rand/v2"
	"reflect"
	"testing"
)

func mergeTestVersion(millis uint64) Version {
	var node [16]byte
	for i := range node {
		node[i] = 1
	}
	return Version{Millis: millis, Node: node}
}

func mergeFixture(count int, policy MergePolicy) ReplicaState {
	schema := Schema{
		Tables: []Table{{
			ID:   "items",
			Name: "items",
			SQL:  "CREATE TABLE items(id INTEGER PRIMARY KEY,n INTEGER,payload BLOB)",
			Columns: []Column{
				{ID: "id", Name: "id", PrimaryKey: 1, DeclaredType: "INTEGER", NotNull: true, Policy: PolicyLww},
				{ID: "n", Name: "n", DeclaredType: "INTEGER", Policy: policy},
				{ID: "payload", Name: "payload", DeclaredType: "BLOB", Policy: PolicyLww},
			},
		}},
	}
	rows := make([]RowState, 0, count)
	for id := 0; id < count; id++ {
		rows = append(rows, RowState{
			Table:      "items",
			Key:        []Value{{Tag: ValueInteger, I: int64(id)}},
			Generation: mergeTestVersion(1),
			Cells: []Cell{
				{Column: "id", Value: Value{Tag: ValueInteger, I: int64(id)}, Version: mergeTestVersion(1)},
				{Column: "n", Value: Value{Tag: ValueInteger, I: 0}, Version: mergeTestVersion(1)},
				{Column: "payload", Value: Value{Tag: ValueBlob, B: bytesRepeat(42, 1024)}, Version: mergeTestVersion(1)},
			},
		})
	}
	var db [16]byte
	for i := range db {
		db[i] = 7
	}
	return ReplicaState{DatabaseID: db, Schema: schema, Rows: rows}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func cloneState(s ReplicaState) ReplicaState {
	cp := s
	cp.Schema.Tables = append([]Table(nil), s.Schema.Tables...)
	cp.Rows = make([]RowState, len(s.Rows))
	for i := range s.Rows {
		cp.Rows[i] = *cloneRow(&s.Rows[i])
	}
	cp.Tombstones = append([]Tombstone(nil), s.Tombstones...)
	return cp
}

func TestBatchJoinMatchesSequential(t *testing.T) {
	for _, policy := range []MergePolicy{PolicyLww, PolicyMin, PolicyMax, PolicyCounter, PolicySet} {
		baseline := mergeFixture(32, policy)
		sequential := cloneState(baseline)
		var states []ReplicaState
		rng := rand.New(rand.NewPCG(777, 0))
		for step := 1; step <= 80; step++ {
			state := cloneState(baseline)
			state.Rows = nil
			state.Tombstones = nil
			id := rng.IntN(32)
			row := *cloneRow(&baseline.Rows[id])
			if rng.IntN(4) == 0 {
				row.Generation = mergeTestVersion(uint64(step + 2))
			}
			row.Cells[1].Version = mergeTestVersion(uint64(step + 100))
			row.Cells[1].Value = Value{Tag: ValueInteger, I: int64(step)}
			dup := *cloneRow(&row)
			state.Rows = []RowState{row, dup} // Duplicate delivery within an envelope.
			if rng.IntN(10) < 4 {
				rowKey := state.Rows[0]
				state.Tombstones = append(state.Tombstones, Tombstone{
					Table:      rowKey.Table,
					Key:        EncodeValues(rowKey.Key),
					Generation: rowKey.Generation,
					Version:    mergeTestVersion(uint64(step + 90)),
				})
			}
			// Deliberately omit precomputed clock maxima. Deleted rows must
			// not inflate the clock beyond sequential-join behavior.
			state.Clock = Version{}
			next, err := Join(&sequential, &state)
			if err != nil {
				t.Fatalf("policy %v step %d join: %v", policy, step, err)
			}
			sequential = *next
			states = append(states, state)
			batched, err := JoinBatch(&baseline, states)
			if err != nil {
				t.Fatalf("policy %v step %d batch: %v", policy, step, err)
			}
			if !reflect.DeepEqual(batched, &sequential) {
				t.Fatalf("policy %v prefix %d: batch != sequential", policy, step)
			}
		}
	}
}

func TestBatchJoinRejects(t *testing.T) {
	baseline := mergeFixture(2, PolicyLww)
	changed := cloneState(baseline)
	changed.Rows = changed.Rows[:1]
	changed.Rows[0].Cells[1].Value = Value{Tag: ValueInteger, I: 99}
	if _, err := JoinBatch(&baseline, []ReplicaState{changed}); err == nil {
		t.Fatal("expected conflict rejection")
	}
	changed = cloneState(baseline)
	changed.Schema.Tables[0].Name = "other"
	if _, err := JoinBatch(&baseline, []ReplicaState{changed}); err != ErrSchemaMismatch {
		t.Fatalf("expected schema mismatch, got %v", err)
	}
	changed = cloneState(baseline)
	changed.Rows[0].Cells = changed.Rows[0].Cells[:2]
	if _, err := JoinBatch(&baseline, []ReplicaState{changed}); err == nil {
		t.Fatal("expected shape rejection")
	}
	same, err := JoinBatch(&baseline, nil)
	if err != nil || !reflect.DeepEqual(same, &baseline) {
		t.Fatalf("empty batch = %+v, %v", same, err)
	}
}

func TestPolicyCompare(t *testing.T) {
	cases := []struct {
		a, b Value
		want int
	}{
		{NullValue(), IntValue(1), -1},
		{IntValue(1), NullValue(), 1},
		{IntValue(1), IntValue(2), -1},
		{IntValue(2), RealValue(2.5), -1},
		{RealValue(2.5), IntValue(2), 1},
		{IntValue(3), RealValue(2.5), 1},
		{IntValue(1 << 62), RealValue(5e18), -1},
		{TextValue("a"), IntValue(1), 1},
		{BlobValue([]byte{1}), TextValue("z"), 1},
	}
	for _, tc := range cases {
		got, err := PolicyCompare(tc.a, tc.b)
		if err != nil || got != tc.want {
			t.Errorf("compare(%v, %v) = %d, %v; want %d", tc.a, tc.b, got, err, tc.want)
		}
	}
	if _, err := PolicyCompare(RealValue(math.NaN()), IntValue(1)); err == nil {
		t.Error("expected NaN rejection")
	}
}

func TestPrepareDelta(t *testing.T) {
	a := mergeFixture(2, PolicyLww)
	b := cloneState(a)
	b.Rows[0].Cells[1].Value = Value{Tag: ValueInteger, I: 7}
	b.Rows[0].Cells[1].Version = mergeTestVersion(50)
	b.Rows = b.Rows[:1]
	b.Tombstones = []Tombstone{{
		Table:      "items",
		Key:        EncodeValues(a.Rows[1].Key),
		Generation: mergeTestVersion(1),
		Version:    mergeTestVersion(60),
	}}
	patch, err := Prepare(&a, &b, 9)
	if err != nil {
		t.Fatal(err)
	}
	if len(patch.Rows) != 1 || len(patch.Deleted) != 1 || len(patch.Tombstones) != 1 {
		t.Fatalf("patch = %+v", patch)
	}
	if patch.Cursor != 9 || patch.State == nil {
		t.Fatalf("patch meta = %+v", patch)
	}
	joined, err := Join(&a, &b)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := PrepareDelta(&a, joined, 3)
	if err != nil || delta.State != nil || delta.Cursor != 3 {
		t.Fatalf("delta = %+v, %v", delta, err)
	}
}

func TestCloneReplicaStateIsolatesBaseline(t *testing.T) {
	base := mergeFixture(1, PolicyLww)
	cloned := CloneReplicaState(&base)
	cloned.Rows[0].Cells[1].Value = IntValue(999)
	cloned.Rows[0].Cells[1].Version = mergeTestVersion(999)
	cloned.Rows[0].Key[0] = IntValue(777)
	if base.Rows[0].Cells[1].Value.I == 999 {
		t.Fatal("clone shares cell backing array with baseline")
	}
	if base.Rows[0].Key[0].I == 777 {
		t.Fatal("clone shares key backing array with baseline")
	}
	patch, err := PrepareDelta(&base, cloned, 1)
	if err != nil {
		t.Fatalf("PrepareDelta: %v", err)
	}
	if len(patch.Rows) != 1 {
		t.Fatalf("PrepareDelta rows = %d, want 1 (baseline was corrupted by clone mutation)", len(patch.Rows))
	}
}

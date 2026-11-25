// Copyright 2026 gorse Project authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package versioning

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gorse-io/gorse/storage/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBacking is an in-memory BackfillStore for deterministic manager tests.
type fakeBacking struct {
	users     []string
	modified  map[string]time.Time
	estimates map[string]int
}

func (f *fakeBacking) AllUsers(_ context.Context) ([]string, error) {
	return append([]string(nil), f.users...), nil
}

func (f *fakeBacking) UserModifiedAt(_ context.Context, userId string) (time.Time, error) {
	return f.modified[userId], nil
}

func (f *fakeBacking) EstimatedItems(_ context.Context, userId string) (int, error) {
	return f.estimates[userId], nil
}

// fakeGenerator records calls and can fail a configured set of users.
type fakeGenerator struct {
	mu        sync.Mutex
	calls     []string
	fail      map[string]bool
	block     chan struct{}
	committed map[string]int64
}

func newFakeGenerator() *fakeGenerator {
	return &fakeGenerator{fail: map[string]bool{}, committed: map[string]int64{}}
}

func (g *fakeGenerator) GenerateForUser(ctx context.Context, userId string, target Version) error {
	if g.block != nil {
		select {
		case <-g.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, userId)
	if g.fail[userId] {
		return assertError("simulated stage failure")
	}
	g.committed[userId] = target.Sequence
	return nil
}

func (g *fakeGenerator) Calls() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.calls...)
}

type errString string

func (e errString) Error() string { return string(e) }

func assertError(msg string) error { return errString(msg) }

func newManagerFixture(t *testing.T) (*Manager, *Store, *fakeBacking) {
	t.Helper()
	client, err := cache.Open("sqlite://"+t.TempDir()+"/cache.db", "")
	require.NoError(t, err)
	require.NoError(t, client.Init())
	t.Cleanup(func() { _ = client.Close() })
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	store := NewStore(client, 3).WithClock(func() time.Time { return now })
	backing := &fakeBacking{
		users:     []string{"u1", "u2", "u3", "u4", "u5"},
		modified:  map[string]time.Time{},
		estimates: map[string]int{"u1": 3, "u2": 2, "u3": 1, "u4": 0, "u5": 4},
	}
	return NewManager(store, backing), store, backing
}

func TestDryRunScopesAndNoWrites(t *testing.T) {
	ctx := context.Background()
	mgr, store, backing := newManagerFixture(t)
	target, _, err := store.GetOrRegisterVersion(ctx, baseComponents())
	require.NoError(t, err)
	_ = target

	// Explicit user scope.
	report, err := mgr.DryRun(ctx, Selector{Scope: ScopeUsers, UserIDs: []string{"u1", "u3", "uX"}})
	require.NoError(t, err)
	assert.Equal(t, 2, report.Candidates)
	assert.Equal(t, 4, report.EstimatedItems)

	// Modified window scope.
	base := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	backing.modified["u2"] = base.Add(30 * time.Minute)
	after := base
	before := base.Add(time.Hour)
	report, err = mgr.DryRun(ctx, Selector{Scope: ScopeModifiedWindow, ModifiedAfter: &after, ModifiedBefore: &before})
	require.NoError(t, err)
	assert.Equal(t, []string{"u2"}, report.ScopedUsers)

	// Dry run must not create any version pointer or task side effects beyond
	// a completed dry-run task.
	pointer, err := store.ReadUserPointer(ctx, "u1")
	require.NoError(t, err)
	assert.Nil(t, pointer)
}

func TestBackfillExecutionIdempotentResumeAndFailures(t *testing.T) {
	ctx := context.Background()
	mgr, store, _ := newManagerFixture(t)
	target, _, err := store.GetOrRegisterVersion(ctx, baseComponents())
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentVersion(ctx, target))

	gen := newFakeGenerator()
	gen.fail["u3"] = true

	task, err := mgr.StartBackfill(ctx, Selector{Scope: ScopeUsers, UserIDs: []string{"u1", "u2", "u3", "u4"}},
		target, false, 2, 100, gen)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got, gErr := mgr.GetTask(ctx, task.ID)
		return gErr == nil && (got.Status == TaskCompleted || got.Status == TaskFailed)
	}, 5*time.Second, 10*time.Millisecond)

	got, err := mgr.GetTask(ctx, task.ID)
	require.NoError(t, err)
	assert.Equal(t, 4, got.Total)
	assert.Equal(t, 3, got.Succeeded)
	assert.Equal(t, 1, got.Failed)
	require.Len(t, got.Failures, 1)
	assert.Equal(t, "u3", got.Failures[0].UserID)
	assert.NotEmpty(t, got.Failures[0].Stage)
	assert.NotEmpty(t, got.Failures[0].Reason)

	// Successful users were committed at the target version.
	assert.Equal(t, target.Sequence, gen.committed["u1"])
	assert.Equal(t, target.Sequence, gen.committed["u4"])
	// The unselected user u5 was never touched.
	assert.NotContains(t, gen.Calls(), "u5")

	// Re-running the same task scope is idempotent: completed users resume from
	// the durable done-set and only the previously failed user is retried.
	gen2 := newFakeGenerator()
	resumed, err := mgr.Resume(ctx, task.ID, gen2)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		g, gErr := mgr.GetTask(ctx, resumed.ID)
		return gErr == nil && g.Status == TaskCompleted
	}, 5*time.Second, 10*time.Millisecond)
	final, _ := mgr.GetTask(ctx, resumed.ID)
	assert.Equal(t, TaskCompleted, final.Status)
	// Only the not-done user (u3) was retried.
	assert.ElementsMatch(t, []string{"u3"}, gen2.Calls())
}

func TestBackfillCancellation(t *testing.T) {
	ctx := context.Background()
	mgr, store, _ := newManagerFixture(t)
	target, _, err := store.GetOrRegisterVersion(ctx, baseComponents())
	require.NoError(t, err)

	gen := newFakeGenerator()
	gen.block = make(chan struct{})

	task, err := mgr.StartBackfill(ctx,
		Selector{Scope: ScopeUsers, UserIDs: []string{"u1", "u2", "u3"}},
		target, false, 1, 100, gen)
	require.NoError(t, err)

	// Wait until the task is actually running (a worker is blocked on the
	// generator) before cancelling, removing start-up timing nondeterminism.
	require.Eventually(t, func() bool {
		got, gErr := mgr.GetTask(ctx, task.ID)
		return gErr == nil && got.Status == TaskRunning
	}, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, mgr.Cancel(ctx, task.ID))

	// Unblock workers; the task must end canceled and keep progress.
	close(gen.block)
	require.Eventually(t, func() bool {
		got, gErr := mgr.GetTask(ctx, task.ID)
		return gErr == nil && got.Status == TaskCanceled
	}, 5*time.Second, 10*time.Millisecond)
}

func TestConcurrentBackfillMergeAndReject(t *testing.T) {
	ctx := context.Background()
	mgr, store, _ := newManagerFixture(t)
	target, _, err := store.GetOrRegisterVersion(ctx, baseComponents())
	require.NoError(t, err)

	gen := newFakeGenerator()
	gen.block = make(chan struct{})

	first, err := mgr.StartBackfill(ctx, Selector{Scope: ScopeUsers, UserIDs: []string{"u1", "u2"}},
		target, false, 1, 100, gen)
	require.NoError(t, err)

	// Identical scope merges into the running task instead of double-writing.
	merged, err := mgr.StartBackfill(ctx, Selector{Scope: ScopeUsers, UserIDs: []string{"u2", "u1"}},
		target, false, 1, 100, gen)
	require.NoError(t, err)
	assert.Equal(t, first.ID, merged.ID)

	// Overlapping scope is explicitly rejected.
	_, err = mgr.StartBackfill(ctx, Selector{Scope: ScopeUsers, UserIDs: []string{"u2", "u3"}},
		target, false, 1, 100, gen)
	require.ErrorIs(t, err, ErrConflictingBackfill)

	// Cancel the still-blocked task and wait for it to release the store before
	// the temp directory is removed, avoiding a cleanup race.
	require.NoError(t, mgr.Cancel(ctx, first.ID))
	close(gen.block)
	require.Eventually(t, func() bool {
		got, gErr := mgr.GetTask(ctx, first.ID)
		return gErr == nil && (got.Status == TaskCanceled || got.Status == TaskCanceling)
	}, 5*time.Second, 10*time.Millisecond)
}

func TestRollbackActivateRejectAndRevert(t *testing.T) {
	ctx := context.Background()
	mgr, store, _ := newManagerFixture(t)
	v1, _, err := store.GetOrRegisterVersion(ctx, baseComponents())
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentVersion(ctx, v1))

	// Give u1/u2 retained v1 rows; u3 has none.
	require.NoError(t, store.CommitResult(ctx, "u1", v1,
		[]cache.Score{{Id: "a", Score: 1}}, false, ""))
	require.NoError(t, store.CommitResult(ctx, "u2", v1,
		[]cache.Score{{Id: "b", Score: 1}}, false, ""))

	// Snapshot state before the rejected call.
	before, err := store.Rollback(ctx)
	require.NoError(t, err)
	assert.Nil(t, before)

	// Target missing for u3 -> reject, state unchanged.
	state, missing, err := mgr.ActivateRollback(ctx, RollbackRequest{
		Sequence: v1.Sequence, UserIDs: []string{"u1", "u2", "u3"},
	})
	require.ErrorIs(t, err, ErrRollbackTargetMissing)
	assert.Nil(t, state)
	require.Len(t, missing, 1)
	assert.Equal(t, "u3", missing[0].UserID)
	after, err := store.Rollback(ctx)
	require.NoError(t, err)
	assert.Nil(t, after, "rollback state must be unchanged after rejection")

	// Valid activation succeeds and the resolver reports rollback.
	state, missing, err = mgr.ActivateRollback(ctx, RollbackRequest{
		Sequence: v1.Sequence, UserIDs: []string{"u1", "u2"}, Reason: "regression",
	})
	require.NoError(t, err)
	assert.Empty(t, missing)
	require.NotNil(t, state)

	resolver := NewResolver(store, time.Hour)
	res, err := resolver.Resolve(ctx, "u1", nil, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, StateRollback, res.Freshness.State)

	// Deactivate restores normal serving.
	require.NoError(t, mgr.DeactivateRollback(ctx, nil))
	res, err = resolver.Resolve(ctx, "u1", nil, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, StateFresh, res.Freshness.State)
}

func TestObserveGroupsCountersAndTasks(t *testing.T) {
	ctx := context.Background()
	mgr, store, backing := newManagerFixture(t)
	v1, _, err := store.GetOrRegisterVersion(ctx, baseComponents())
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentVersion(ctx, v1))

	// u1 fresh on v1, u2 with a stale older pointer (simulate via direct commit
	// of v1 then advance current), u3 missing.
	require.NoError(t, store.CommitResult(ctx, "u1", v1,
		[]cache.Score{{Id: "a"}}, false, ""))
	require.NoError(t, store.CommitResult(ctx, "u2", v1,
		[]cache.Score{{Id: "b"}}, false, ""))

	// Register a newer current version so u1/u2 become stale.
	comp2 := baseComponents()
	comp2.ModelID = 99
	v2, _, err := store.GetOrRegisterVersion(ctx, comp2)
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentVersion(ctx, v2))

	// u4 gets the current version -> fresh.
	require.NoError(t, store.CommitResult(ctx, "u4", v2,
		[]cache.Score{{Id: "c"}}, false, ""))

	// Rollback for u1 only.
	require.NoError(t, store.SetRollback(ctx, RollbackState{Sequence: v1.Sequence, Scope: []string{"u1"}}))

	// One interrupted backfill task recorded.
	task := &Task{
		ID: "bf-interrupted", Selector: Selector{Scope: ScopeUsers, UserIDs: backing.users},
		Target: v2, Status: TaskCanceled, Total: 5, Succeeded: 1, Failed: 0,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	require.NoError(t, mgr.persistTask(ctx, task))

	snapshot, err := mgr.Observe(ctx, time.Hour, 10)
	require.NoError(t, err)
	assert.True(t, snapshot.RollbackActive)
	assert.Equal(t, v2.Sequence, snapshot.CurrentSequence)
	// 5 users: u1 rollback, u2 stale(v1), u3 missing, u4 fresh(v2), u5 missing.
	assert.Equal(t, 5, snapshot.Totals.Total)
	assert.Equal(t, 1, snapshot.Totals.Rollback)
	assert.Equal(t, 1, snapshot.Totals.Fresh)
	assert.Equal(t, 1, snapshot.Totals.Stale)
	assert.Equal(t, 2, snapshot.Totals.Missing)
	require.Len(t, snapshot.LatestTasks, 1)
	assert.Equal(t, TaskCanceled, snapshot.LatestTasks[0].Status)
}

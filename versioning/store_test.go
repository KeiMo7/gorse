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
	"testing"
	"time"

	"github.com/gorse-io/gorse/storage/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// controllableStore returns a store whose clock can be advanced on demand.
func controllableStore(t *testing.T) (*Store, *time.Time) {
	t.Helper()
	client, err := cache.Open("sqlite://"+t.TempDir()+"/cache.db", "")
	require.NoError(t, err)
	require.NoError(t, client.Init())
	t.Cleanup(func() { _ = client.Close() })
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	store := NewStore(client, 3).WithClock(func() time.Time { return now })
	return store, &now
}

func registerCurrent(t *testing.T, ctx context.Context, store *Store, comp Components) Version {
	t.Helper()
	v, _, err := store.GetOrRegisterVersion(ctx, comp)
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentVersion(ctx, v))
	return v
}

func TestCommitResultAndVersionedRead(t *testing.T) {
	ctx := context.Background()
	store, now := controllableStore(t)
	comp := baseComponents()
	v1 := registerCurrent(t, ctx, store, comp)

	results := []cache.Score{
		{Id: "a", Score: 0.9, Timestamp: *now},
		{Id: "b", Score: 0.8, Timestamp: *now},
	}
	require.NoError(t, store.CommitResult(ctx, "u1", v1, results, true, "legacy-digest"))

	// Versioned subset is readable.
	scores, err := store.cache.SearchScores(ctx, RecommendVersioned, Subset("u1", v1.Sequence), nil, 0, -1)
	require.NoError(t, err)
	require.Len(t, scores, 2)
	assert.Equal(t, "a", scores[0].Id)

	// Pointer reflects the committed version.
	pointer, err := store.ReadUserPointer(ctx, "u1")
	require.NoError(t, err)
	require.NotNil(t, pointer)
	assert.Equal(t, v1.Sequence, pointer.Sequence)
	assert.Equal(t, v1.Fingerprint, pointer.Fingerprint)

	// Legacy mirror keeps the existing keys populated for backward compatibility.
	legacy, err := store.cache.SearchScores(ctx, cache.Recommend, "u1", nil, 0, -1)
	require.NoError(t, err)
	require.Len(t, legacy, 2)
	digest, exists, err := store.readString(ctx, cache.Key(cache.RecommendDigest, "u1"))
	require.NoError(t, err)
	require.True(t, exists)
	assert.Equal(t, "legacy-digest", digest)
}

func TestFreshnessFourStates(t *testing.T) {
	ctx := context.Background()
	store, now := controllableStore(t)
	ageLimit := time.Hour
	resolver := NewResolver(store, ageLimit)

	comp1 := baseComponents()
	v1 := registerCurrent(t, ctx, store, comp1)

	// MISSING: no pointer yet.
	res, err := resolver.Resolve(ctx, "ghost", nil, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, StateMissing, res.Freshness.State)
	assert.Empty(t, res.Scores)

	// Commit v1 for u1 -> FRESH.
	require.NoError(t, store.CommitResult(ctx, "u1", v1,
		[]cache.Score{{Id: "a", Score: 1, Timestamp: *now}}, false, ""))
	res, err = resolver.Resolve(ctx, "u1", nil, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, StateFresh, res.Freshness.State)
	assert.Equal(t, int64(0), res.Freshness.VersionsBehind)
	assert.Len(t, res.Scores, 1)

	// Advance the clock beyond the age limit -> STALE by age.
	*now = now.Add(2 * ageLimit)
	res, err = resolver.Resolve(ctx, "u1", nil, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, StateStale, res.Freshness.State)
	assert.Contains(t, res.Freshness.Reason, "maximum data age")

	// Register and activate a new version -> STALE by version, one behind.
	*now = time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	comp2 := baseComponents()
	comp2.ModelID = 43
	v2 := registerCurrent(t, ctx, store, comp2)
	res, err = resolver.Resolve(ctx, "u1", nil, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, StateStale, res.Freshness.State)
	assert.Equal(t, int64(1), res.Freshness.VersionsBehind)
	assert.Equal(t, v2.Sequence, res.Freshness.ExpectedSequence)
	assert.Equal(t, v1.Sequence, res.Freshness.ActualSequence)

	// Commit v2 -> FRESH again.
	require.NoError(t, store.CommitResult(ctx, "u1", v2,
		[]cache.Score{{Id: "c", Score: 1, Timestamp: *now}}, false, ""))
	res, err = resolver.Resolve(ctx, "u1", nil, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, StateFresh, res.Freshness.State)
}

func TestRollbackToPreviousVersion(t *testing.T) {
	ctx := context.Background()
	store, now := controllableStore(t)
	resolver := NewResolver(store, time.Hour)

	v1 := registerCurrent(t, ctx, store, baseComponents())
	require.NoError(t, store.CommitResult(ctx, "u1", v1,
		[]cache.Score{{Id: "old", Score: 1, Timestamp: *now}}, false, ""))

	comp2 := baseComponents()
	comp2.ModelID = 43
	v2 := registerCurrent(t, ctx, store, comp2)
	require.NoError(t, store.CommitResult(ctx, "u1", v2,
		[]cache.Score{{Id: "new", Score: 1, Timestamp: *now}}, false, ""))

	// Roll back globally to v1: reader sees old results with rollback marker.
	require.NoError(t, store.SetRollback(ctx, RollbackState{
		Sequence: v1.Sequence, Reason: "model regression", StartedAt: *now,
	}))
	res, err := resolver.Resolve(ctx, "u1", nil, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, StateRollback, res.Freshness.State)
	assert.True(t, res.Effective.Rollback)
	require.Len(t, res.Scores, 1)
	assert.Equal(t, "old", res.Scores[0].Id)

	// Clear rollback -> current v2 restored.
	require.NoError(t, store.ClearRollback(ctx))
	res, err = resolver.Resolve(ctx, "u1", nil, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, StateFresh, res.Freshness.State)
	assert.False(t, res.Effective.Rollback)
	require.Len(t, res.Scores, 1)
	assert.Equal(t, "new", res.Scores[0].Id)
}

func TestRollbackTargetMissingIsRejected(t *testing.T) {
	ctx := context.Background()
	store, _ := controllableStore(t)
	v1 := registerCurrent(t, ctx, store, baseComponents())

	// User has no rows for v1.
	exists, err := store.HasVersionedResult(ctx, "u9", v1.Sequence)
	require.NoError(t, err)
	assert.False(t, exists)

	// Activating rollback itself is allowed, but the read verdict must not
	// pretend to serve a non-existent target: it degrades to missing/stale.
	require.NoError(t, store.SetRollback(ctx, RollbackState{Sequence: v1.Sequence, StartedAt: time.Now().UTC()}))
	resolver := NewResolver(store, time.Hour)
	res, err := resolver.Resolve(ctx, "u9", nil, 0, -1)
	require.NoError(t, err)
	assert.True(t, res.Effective.Rollback)
	assert.Contains(t, []string{StateMissing, StateStale}, res.Freshness.State)
	assert.Empty(t, res.Scores)
}

func TestCorruptMetadataDegradesToStale(t *testing.T) {
	ctx := context.Background()
	store, _ := controllableStore(t)
	registerCurrent(t, ctx, store, baseComponents())

	// Write garbage into the user pointer.
	require.NoError(t, store.cache.Set(ctx, cache.String(userPointerKey("u1"), "{not-json")))
	resolver := NewResolver(store, time.Hour)
	res, err := resolver.Resolve(ctx, "u1", nil, 0, -1)
	require.NoError(t, err)
	// The request does not fail; the verdict is conservatively stale with a reason.
	assert.Equal(t, StateStale, res.Freshness.State)
	assert.NotEmpty(t, res.Freshness.Reason)
	assert.Empty(t, res.Scores)
}

func TestHistoryBoundedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	store, now := controllableStore(t)

	var versions []Version
	for i := int64(1); i <= 5; i++ {
		comp := baseComponents()
		comp.ModelID = 100 + i
		v := registerCurrent(t, ctx, store, comp)
		versions = append(versions, v)
		require.NoError(t, store.CommitResult(ctx, "u1", v,
			[]cache.Score{{Id: "x", Score: float64(i), Timestamp: *now}}, false, ""))
	}
	pointer, err := store.ReadUserPointer(ctx, "u1")
	require.NoError(t, err)
	// History is bounded by the store limit.
	assert.Len(t, pointer.History, 3)
	assert.Equal(t, versions[4].Sequence, pointer.History[0].Sequence)

	// Re-committing the same version is idempotent and does not grow history.
	require.NoError(t, store.CommitResult(ctx, "u1", versions[4],
		[]cache.Score{{Id: "y", Score: 1, Timestamp: *now}}, false, ""))
	pointer, err = store.ReadUserPointer(ctx, "u1")
	require.NoError(t, err)
	assert.Len(t, pointer.History, 3)
	assert.Equal(t, versions[4].Sequence, pointer.Sequence)
}

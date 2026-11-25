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

package server

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/gorse-io/gorse/versioning"
	"github.com/steinfletcher/apitest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// versioningSuite builds a server suite with a versioning store plus seed data.
func (suite *ServerTestSuite) setupVersionedUser(t *testing.T, userId string, comp versioning.Components, seq int64, items []cache.Score) versioning.Version {
	t.Helper()
	ctx := context.Background()
	store := versioning.NewStore(suite.CacheClient, 3)
	version, _, err := store.GetOrRegisterVersion(ctx, comp)
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentVersion(ctx, version))
	require.NoError(t, store.CommitResult(ctx, userId, version, items, true, "digest"))
	return version
}

func (suite *ServerTestSuite) TestRecommendExplainFreshAndStale() {
	ctx := suite.T().Context()
	// Seed items so fallback "latest" has data and offline rows are valid items.
	items := []data.Item{
		{ItemId: "a", Timestamp: time.Now()},
		{ItemId: "b", Timestamp: time.Now()},
		{ItemId: "c", Timestamp: time.Now()},
	}
	require.NoError(suite.T(), suite.DataClient.BatchInsertItems(ctx, items))

	comp := versioning.Components{
		RecallDigest:  "r1",
		RankerType:    "none",
		RankerDigest:  "k1",
		SampleCutoff:  time.Now().Add(-time.Hour).UTC(),
		FallbackChain: []string{"latest"},
	}
	v := suite.setupVersionedUser(suite.T(), "fresh-user", comp, 1,
		[]cache.Score{{Id: "a", Score: 2}, {Id: "b", Score: 1}})

	// FRESH explain response. With n=2 the offline set fully satisfies the
	// request, so no fallback augmentation occurs.
	var fresh struct {
		Items       []string           `json:"items"`
		Explanation versioning.Explain `json:"explanation"`
	}
	apitest.New().Handler(suite.handler).
		Get("/api/recommend/fresh-user/explain").
		Header("X-API-Key", apiKey).
		Query("n", "2").
		Expect(suite.T()).
		Status(http.StatusOK).
		End().JSON(&fresh)
	assert.Equal(suite.T(), []string{"a", "b"}, fresh.Items)
	assert.Equal(suite.T(), versioning.StateFresh, fresh.Explanation.Freshness.State)
	assert.Equal(suite.T(), v.Sequence, fresh.Explanation.Version.Sequence)
	assert.False(suite.T(), fresh.Explanation.Fallback.Used)
	assert.Contains(suite.T(), fresh.Explanation.Sources, "a")
	assert.Equal(suite.T(), "offline", fresh.Explanation.Sources["a"])

	// With a larger n, fallback ("latest") supplements the short offline set.
	var supplemented struct {
		Items       []string           `json:"items"`
		Explanation versioning.Explain `json:"explanation"`
	}
	apitest.New().Handler(suite.handler).
		Get("/api/recommend/fresh-user/explain").
		Header("X-API-Key", apiKey).
		Query("n", "10").
		Expect(suite.T()).
		Status(http.StatusOK).
		End().JSON(&supplemented)
	assert.True(suite.T(), supplemented.Explanation.Fallback.Used)
	assert.GreaterOrEqual(suite.T(), supplemented.Explanation.Online.SupplementCount, 1)
	assert.Equal(suite.T(), "online/latest", supplemented.Explanation.Sources["c"])

	// Advance to a new current version without regenerating the user -> STALE.
	store := versioning.NewStore(suite.CacheClient, 3)
	comp2 := comp
	comp2.ModelID = 55
	v2, _, err := store.GetOrRegisterVersion(ctx, comp2)
	require.NoError(suite.T(), err)
	require.NoError(suite.T(), store.SetCurrentVersion(ctx, v2))

	var stale struct {
		Explanation versioning.Explain `json:"explanation"`
	}
	apitest.New().Handler(suite.handler).
		Get("/api/recommend/fresh-user/explain").
		Header("X-API-Key", apiKey).
		Expect(suite.T()).
		Status(http.StatusOK).
		End().JSON(&stale)
	assert.Equal(suite.T(), versioning.StateStale, stale.Explanation.Freshness.State)
	assert.Equal(suite.T(), int64(1), stale.Explanation.Freshness.VersionsBehind)
}

func (suite *ServerTestSuite) TestRecommendExplainMissingAndRollback() {
	ctx := suite.T().Context()
	require.NoError(suite.T(), suite.DataClient.BatchInsertItems(ctx, []data.Item{
		{ItemId: "x", Timestamp: time.Now()},
	}))
	comp := versioning.Components{
		RecallDigest: "r", RankerType: "none", RankerDigest: "k",
		SampleCutoff: time.Now().Add(-time.Hour).UTC(), FallbackChain: []string{"latest"},
	}
	v := suite.setupVersionedUser(suite.T(), "rb-user", comp, 1,
		[]cache.Score{{Id: "x", Score: 1}})

	// MISSING for a user that was never generated.
	var missing struct {
		Explanation versioning.Explain `json:"explanation"`
	}
	apitest.New().Handler(suite.handler).
		Get("/api/recommend/ghost-user/explain").
		Header("X-API-Key", apiKey).
		Expect(suite.T()).
		Status(http.StatusOK).
		End().JSON(&missing)
	assert.Equal(suite.T(), versioning.StateMissing, missing.Explanation.Freshness.State)
	assert.True(suite.T(), missing.Explanation.Fallback.Used)

	// Activate rollback to v1 and confirm the marker is present.
	store := versioning.NewStore(suite.CacheClient, 3)
	require.NoError(suite.T(), store.SetRollback(ctx, versioning.RollbackState{
		Sequence: v.Sequence, Reason: "test", StartedAt: time.Now().UTC(),
	}))
	var rolled struct {
		Explanation versioning.Explain `json:"explanation"`
	}
	apitest.New().Handler(suite.handler).
		Get("/api/recommend/rb-user/explain").
		Header("X-API-Key", apiKey).
		Expect(suite.T()).
		Status(http.StatusOK).
		End().JSON(&rolled)
	assert.Equal(suite.T(), versioning.StateRollback, rolled.Explanation.Freshness.State)
	assert.True(suite.T(), rolled.Explanation.Version.Rollback)
}

func (suite *ServerTestSuite) TestLegacyRecommendFieldsUnchanged() {
	ctx := suite.T().Context()
	require.NoError(suite.T(), suite.DataClient.BatchInsertItems(ctx, []data.Item{
		{ItemId: "p", Timestamp: time.Now()},
		{ItemId: "q", Timestamp: time.Now()},
	}))
	comp := versioning.Components{
		RecallDigest: "r", RankerType: "none", RankerDigest: "k",
		SampleCutoff: time.Now().Add(-time.Hour).UTC(), FallbackChain: []string{"latest"},
	}
	suite.setupVersionedUser(suite.T(), "legacy-user", comp, 1,
		[]cache.Score{{Id: "p", Score: 2}, {Id: "q", Score: 1}})

	// The legacy endpoint keeps the exact bare string-array body and 200 status;
	// only additive headers are introduced. Pin n to the offline set size so the
	// assertion covers the unchanged offline ordering without fallback.
	var body []string
	apitest.New().Handler(suite.handler).
		Get("/api/recommend/legacy-user").
		Header("X-API-Key", apiKey).
		Query("n", "2").
		Expect(suite.T()).
		Status(http.StatusOK).
		Assert(func(res *http.Response, _ *http.Request) error {
			assert.Equal(suite.T(), "fresh", res.Header.Get("X-Gorse-Freshness"))
			assert.NotEmpty(suite.T(), res.Header.Get("X-Gorse-Version-Sequence"))
			return nil
		}).
		End().JSON(&body)
	assert.Equal(suite.T(), []string{"p", "q"}, body)

	// X-API-Version 2 still returns score objects, unchanged.
	var scores []cache.Score
	apitest.New().Handler(suite.handler).
		Get("/api/recommend/legacy-user").
		Header("X-API-Key", apiKey).
		Header("X-API-Version", "2").
		Query("n", "2").
		Expect(suite.T()).
		Status(http.StatusOK).
		End().JSON(&scores)
	require.Len(suite.T(), scores, 2)
	assert.Equal(suite.T(), "p", scores[0].Id)
}

func (suite *ServerTestSuite) TestCorruptPointerDoesNotFailRequest() {
	// Register a current version but corrupt the user pointer.
	store := versioning.NewStore(suite.CacheClient, 3)
	comp := versioning.Components{RecallDigest: "r", RankerType: "none", RankerDigest: "k",
		SampleCutoff: time.Now().Add(-time.Hour).UTC(), FallbackChain: []string{"latest"}}
	_, _, err := store.GetOrRegisterVersion(suite.T().Context(), comp)
	require.NoError(suite.T(), err)
	require.NoError(suite.T(), suite.CacheClient.Set(suite.T().Context(),
		cache.String(cache.Key(versioning.UserPointerPrefix, "broken"), "{bad")))

	apitest.New().Handler(suite.handler).
		Get("/api/recommend/broken/explain").
		Header("X-API-Key", apiKey).
		Expect(suite.T()).
		Status(http.StatusOK).
		End()
}

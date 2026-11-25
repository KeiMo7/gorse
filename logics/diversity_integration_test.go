// Copyright 2025 gorse Project Authors
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

package logics

import (
	"fmt"
	"testing"
	"time"

	"github.com/gorse-io/gorse/common/expression"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/gorse-io/gorse/storage/vectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// diversityStore bundles the embedded stores used by the integration tests.
// Everything lives under t.TempDir(), with no external service dependency.
type diversityStore struct {
	data   data.Database
	cache  cache.Database
	vector vectors.Database
}

func newDiversityStore(t *testing.T) *diversityStore {
	t.Helper()
	dir := t.TempDir()
	dataClient, err := data.Open(fmt.Sprintf("sqlite://%s/data.db", dir), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataClient.Close() })
	cacheClient, err := cache.Open(fmt.Sprintf("sqlite://%s/cache.db", dir), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = cacheClient.Close() })
	vectorClient, err := vectors.Open(fmt.Sprintf("xvec://%s/vectors", dir), "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = vectorClient.Close() })
	require.NoError(t, dataClient.Init())
	require.NoError(t, cacheClient.Init())
	require.NoError(t, vectorClient.Init())
	return &diversityStore{data: dataClient, cache: cacheClient, vector: vectorClient}
}

// diversityFixture inserts 12 items: 8 "movie" (four series of trailer/full
// pairs) and 4 "music", plus the given feedback for the user.
func diversityFixture(t *testing.T, store *diversityStore, userId string, feedback []data.Feedback) config.RecommendConfig {
	t.Helper()
	ctx := t.Context()
	now := time.Unix(1_700_000_000, 0).UTC()
	items := make([]data.Item, 0, 12)
	// Four movie series, each with two entries sharing the dedup key.
	for i := range 4 {
		series := fmt.Sprintf("series-%d", i)
		for _, kind := range []string{"trailer", "full"} {
			itemId := fmt.Sprintf("movie-%d-%s", i, kind)
			items = append(items, data.Item{
				ItemId:     itemId,
				Categories: []string{"movie"},
				Timestamp:  now,
				Labels:     map[string]any{"series_id": series, "kind": kind},
			})
		}
	}
	for i := range 4 {
		items = append(items, data.Item{
			ItemId:     fmt.Sprintf("music-%d", i),
			Categories: []string{"music"},
			Timestamp:  now,
			Labels:     map[string]any{"series_id": fmt.Sprintf("music-series-%d", i)},
		})
	}
	require.NoError(t, store.data.BatchInsertItems(ctx, items))
	require.NoError(t, store.data.BatchInsertUsers(ctx, []data.User{{UserId: userId}}))
	if len(feedback) > 0 {
		require.NoError(t, store.data.BatchInsertFeedback(ctx, feedback, true, true, false))
	}
	cfg := config.GetDefaultConfig().Recommend
	cfg.CacheSize = 50
	cfg.DataSource.PositiveFeedbackTypes = []expression.FeedbackTypeExpression{
		{FeedbackType: "read"},
	}
	cfg.DataSource.NegativeFeedbackTypes = []expression.FeedbackTypeExpression{
		{FeedbackType: "dislike"},
	}
	cfg.Ranker.Type = "none"
	cfg.Ranker.Recommenders = []string{"latest"}
	return cfg
}

// TestIntegrationDiversityFoldingAcrossLinks verifies that the same series'
// trailer/full entries are folded into one representative through the real
// Recommender entry point backed by embedded stores.
func TestIntegrationDiversityFoldingAcrossLinks(t *testing.T) {
	store := newDiversityStore(t)
	cfg := diversityFixture(t, store, "alice", nil)
	cfg.Diversity = config.DiversityConfig{
		DedupKey:   "item.Labels.series_id",
		MultiLabel: config.MultiLabelAll,
		Categories: []config.CategoryBudget{{
			Name: "movie", MaxCount: 4, MaxRatio: 0.6, MaxConsecutive: 3,
		}},
	}
	recommender, err := NewRecommender(cfg, store.cache, store.data, store.vector, true, "alice", nil)
	require.NoError(t, err)
	scores, err := recommender.Recommend(t.Context(), 12)
	require.NoError(t, err)
	require.NotNil(t, recommender.LastConstraintReport)
	report := recommender.LastConstraintReport
	// 12 items -> 8 distinct groups (4 movie series + 4 music series).
	assert.Equal(t, 12, report.Candidates)
	assert.Equal(t, 4, report.Folded)
	ids := idsOf(scores)
	assert.Len(t, ids, 8)
	assert.Len(t, ids, len(loUniq(ids)))
	// No two survivors share a series.
	seenSeries := make(map[string]struct{})
	itemsMap, err := recommender.LoadConstraintItems(t.Context(), scores)
	require.NoError(t, err)
	for _, score := range scores {
		series := itemsMap[score.Id].Labels.(map[string]any)["series_id"].(string)
		_, dup := seenSeries[series]
		assert.False(t, dup, "series %s appeared twice", series)
		seenSeries[series] = struct{}{}
	}
	// Movie cap honored.
	movieCount := 0
	for _, score := range scores {
		if len(itemsMap[score.Id].Categories) > 0 && itemsMap[score.Id].Categories[0] == "movie" {
			movieCount++
		}
	}
	assert.LessOrEqual(t, movieCount, 4)
}

// TestIntegrationDiversityTwoSidesAndPagination simulates the generation
// (worker) and serving (master) sides sharing one engine: after generation,
// part of the output is marked consumed; the serving re-validation must still
// satisfy the budgets and adjacent pages must not repeat or skip items.
func TestIntegrationDiversityTwoSidesAndPagination(t *testing.T) {
	store := newDiversityStore(t)
	userId := "bob"
	cfg := diversityFixture(t, store, userId, nil)
	cfg.Diversity = config.DiversityConfig{
		DedupKey:   "item.Labels.series_id",
		MultiLabel: config.MultiLabelAll,
		Categories: []config.CategoryBudget{{
			Name: "movie", MaxCount: 4, MaxRatio: 0.8, MaxConsecutive: 3,
		}},
	}
	ctx := t.Context()

	// Generation side: constrain the full ranked sequence once.
	genRecommender, err := NewRecommender(cfg, store.cache, store.data, store.vector, false, userId, nil)
	require.NoError(t, err)
	generated, err := genRecommender.Recommend(ctx, 0)
	require.NoError(t, err)
	require.NotNil(t, genRecommender.LastConstraintReport)
	assert.False(t, genRecommender.LastConstraintReport.Exhausted)
	genItems, err := genRecommender.LoadConstraintItems(ctx, generated)
	require.NoError(t, err)

	// The user consumes the first two generated items (group diffusion).
	consumedFeedback := []data.Feedback{
		{UserId: userId, ItemId: generated[0].Id, FeedbackType: "read", Timestamp: time.Now()},
		{UserId: userId, ItemId: generated[1].Id, FeedbackType: "read", Timestamp: time.Now()},
	}
	require.NoError(t, store.data.BatchInsertFeedback(ctx, consumedFeedback, true, true, false))

	// Serving side: a fresh recommender sees the new consumed-state and
	// re-validates the generated list with the same engine/config. Two read
	// items are excluded by the legacy read-feedback rule themselves, while
	// their whole groups diffuse away through the constraint engine.
	servingRecommender, err := NewRecommender(cfg, store.cache, store.data, store.vector, true, userId, nil)
	require.NoError(t, err)
	fullWindow, err := servingRecommender.Recommend(ctx, 8)
	require.NoError(t, err)
	require.NotNil(t, servingRecommender.LastConstraintReport)
	servedIds := idsOf(fullWindow)
	assert.Len(t, servedIds, len(loUniq(servedIds)))
	// Consumed groups must have diffused: neither consumed item nor any group
	// member reappears.
	for _, score := range generated[:2] {
		consumedSeries := genItems[score.Id].Labels.(map[string]any)["series_id"].(string)
		for _, served := range fullWindow {
			servedSeries := genItems[served.Id].Labels.(map[string]any)["series_id"].(string)
			assert.NotEqual(t, consumedSeries, servedSeries)
		}
	}
	require.GreaterOrEqual(t, len(fullWindow), 4)
	// Pagination over one constrained sequence: request the full window in one
	// deterministic call, then slice it into adjacent pages. Because budgets
	// are computed over the whole request position rather than reset per page,
	// adjacent pages are disjoint and their concatenation is the window.
	windowRecommender, err := NewRecommender(cfg, store.cache, store.data, store.vector, true, userId, nil)
	require.NoError(t, err)
	window2, err := windowRecommender.Recommend(ctx, len(fullWindow))
	require.NoError(t, err)
	require.Len(t, window2, len(fullWindow))
	pageSize := len(window2) / 2
	page1, page2 := window2[:pageSize], window2[pageSize:]
	assert.NotEmpty(t, page1)
	assert.NotEmpty(t, page2)
	page1Ids := idsOf(page1)
	for _, score := range page2 {
		assert.NotContains(t, page1Ids, score.Id)
	}
	// The two pages concatenate back to the full constrained window.
	assert.Equal(t, servedIds, append(idsOf(page1), idsOf(page2)...))
	// After two music groups were consumed, the remaining pool is dominated
	// by movies. Whatever the scheduler had to relax, the hard movie count cap
	// still holds and every emitted item is unique and budget-feasible.
	movieCount := 0
	for _, score := range window2 {
		item := genItems[score.Id]
		if len(item.Categories) > 0 && item.Categories[0] == "movie" {
			movieCount++
		}
	}
	assert.LessOrEqual(t, movieCount, 4)
	assert.LessOrEqual(t, windowRecommender.LastConstraintReport.RelaxLevel, RelaxLevelCount)
}

// TestIntegrationDiversityCompatibility verifies that with no dedup key and no
// budgets configured, the constrained output is item-by-item identical to the
// legacy output (content, order, count) and no report is produced.
func TestIntegrationDiversityCompatibility(t *testing.T) {
	store := newDiversityStore(t)
	// Legacy configuration: diversity section entirely absent.
	legacyCfg := diversityFixture(t, store, "carol", nil)
	require.False(t, legacyCfg.Diversity.Enabled())

	legacyRecommender, err := NewRecommender(legacyCfg, store.cache, store.data, store.vector, true, "carol", nil)
	require.NoError(t, err)
	legacyScores, err := legacyRecommender.Recommend(t.Context(), 10)
	require.NoError(t, err)
	assert.Nil(t, legacyRecommender.LastConstraintReport)

	// A second run with an explicit-but-empty diversity config must match.
	emptyCfg := legacyCfg
	emptyCfg.Diversity = config.DiversityConfig{}
	emptyRecommender, err := NewRecommender(emptyCfg, store.cache, store.data, store.vector, true, "carol", nil)
	require.NoError(t, err)
	emptyScores, err := emptyRecommender.Recommend(t.Context(), 10)
	require.NoError(t, err)
	assert.Nil(t, emptyRecommender.LastConstraintReport)
	require.Equal(t, len(legacyScores), len(emptyScores))
	for i := range legacyScores {
		assert.Equal(t, legacyScores[i].Id, emptyScores[i].Id)
		assert.Equal(t, legacyScores[i].Score, emptyScores[i].Score)
	}
	// Determinism: another identical run yields the same sequence.
	repeatRecommender, err := NewRecommender(legacyCfg, store.cache, store.data, store.vector, true, "carol", nil)
	require.NoError(t, err)
	repeatScores, err := repeatRecommender.Recommend(t.Context(), 10)
	require.NoError(t, err)
	assert.Equal(t, idsOf(legacyScores), idsOf(repeatScores))
}

// TestIntegrationDiversityExhaustionReason verifies the request returns the
// actual feasible count with an explicit reason instead of silently filling
// the page by lowering the count cap.
func TestIntegrationDiversityExhaustionReason(t *testing.T) {
	store := newDiversityStore(t)
	cfg := diversityFixture(t, store, "dave", nil)
	cfg.Diversity = config.DiversityConfig{
		DedupKey:   "item.Labels.series_id",
		MultiLabel: config.MultiLabelAll,
		Categories: []config.CategoryBudget{{
			Name: "movie", MaxCount: 1, MaxRatio: 0.2, MaxConsecutive: 1,
		}},
	}
	recommender, err := NewRecommender(cfg, store.cache, store.data, store.vector, true, "dave", nil)
	require.NoError(t, err)
	// Request more than the budgets can admit: 8 groups exist but only one
	// movie may survive alongside the 4 music groups.
	scores, err := recommender.Recommend(t.Context(), 10)
	require.NoError(t, err)
	report := recommender.LastConstraintReport
	require.NotNil(t, report)
	assert.True(t, report.Exhausted)
	assert.NotEmpty(t, report.ExhaustedReason)
	// Feasible output stays below the request, never duplicated.
	assert.Equal(t, len(scores), report.Returned)
	assert.Less(t, len(scores), 10)
	assert.Len(t, idsOf(scores), len(loUniq(idsOf(scores))))
	// Observability counters agree with the actual output.
	totalFromStats := 0
	for _, stat := range report.CategoryStats {
		totalFromStats += stat.Count
	}
	assert.Equal(t, len(scores), totalFromStats)
}

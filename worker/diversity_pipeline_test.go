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

package worker

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gorse-io/gorse/common/expression"
	"github.com/gorse-io/gorse/common/log"
	"github.com/gorse-io/gorse/common/monitor"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/logics"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/gorse-io/gorse/storage/vectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPipelineDiversityConstraint runs the generation pipeline over embedded
// stores and verifies that the cached recommendation is de-duplicated, the
// folding report is persisted next to it, and the stored sequence matches
// what the serving-side engine would produce.
func TestPipelineDiversityConstraint(t *testing.T) {
	log.SetTestLogger(t)
	ctx := t.Context()
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

	// 6 items in 3 series (trailer/full pairs), all "movie", plus one user
	// with positive feedback so the user is not cold-start.
	now := time.Unix(1_700_000_000, 0).UTC()
	items := make([]data.Item, 0, 6)
	for i := range 3 {
		for _, kind := range []string{"trailer", "full"} {
			items = append(items, data.Item{
				ItemId:     fmt.Sprintf("movie-%d-%s", i, kind),
				Categories: []string{"movie"},
				Timestamp:  now,
				Labels:     map[string]any{"series_id": fmt.Sprintf("series-%d", i)},
			})
		}
	}
	require.NoError(t, dataClient.BatchInsertItems(ctx, items))
	require.NoError(t, dataClient.BatchInsertUsers(ctx, []data.User{{UserId: "zoe"}}))
	require.NoError(t, dataClient.BatchInsertFeedback(ctx, []data.Feedback{
		{UserId: "zoe", ItemId: "movie-0-trailer", FeedbackType: "read", Timestamp: now},
	}, true, true, false))

	cfg := config.GetDefaultConfig()
	cfg.Recommend.CacheSize = 50
	cfg.Recommend.DataSource.PositiveFeedbackTypes = []expression.FeedbackTypeExpression{
		{FeedbackType: "read"},
	}
	cfg.Recommend.Ranker.Type = "none"
	cfg.Recommend.Ranker.Recommenders = []string{"latest"}
	cfg.Recommend.Diversity = config.DiversityConfig{
		DedupKey:   "item.Labels.series_id",
		MultiLabel: config.MultiLabelAll,
		Categories: []config.CategoryBudget{{
			Name: "movie", MaxCount: 3, MaxRatio: 0.8, MaxConsecutive: 3,
		}},
	}

	pipeline := &Pipeline{
		Config:       cfg,
		DataClient:   dataClient,
		CacheClient:  cacheClient,
		VectorClient: vectorClient,
		Tracer:       monitor.NewTracer("test"),
		Jobs:         1,
	}
	pipeline.Recommend(ctx, []data.User{{UserId: "zoe"}}, nil)

	// The cached recommendation must contain one representative per series.
	cached, err := cacheClient.SearchScores(ctx, cache.Recommend, "zoe", nil, 0, -1)
	require.NoError(t, err)
	require.Len(t, cached, 2) // series-0 was consumed (diffusion), series-1/2 remain
	ids := cache.ConvertDocumentsToValues(cached)
	assert.Len(t, ids, len(unique(ids)))
	for _, id := range ids {
		assert.NotContains(t, id, "movie-0-")
	}

	// The folding report must be persisted alongside the sequence and parse.
	reportBytes, err := cacheClient.Get(ctx, cache.Key(cache.RecommendConstraintReport, "zoe")).String()
	require.NoError(t, err)
	require.NotEmpty(t, reportBytes)
	var report logics.ConstraintReport
	require.NoError(t, json.Unmarshal([]byte(reportBytes), &report))
	assert.Equal(t, cfg.Recommend.Diversity.Hash(), report.ConfigHash)
	assert.Greater(t, report.Folded, 0)
	assert.Equal(t, len(cached), report.Returned)
	// The report must not leak raw labels or comments.
	assert.NotContains(t, reportBytes, "trailer")
	assert.NotContains(t, reportBytes, "series-0")
}

func unique(ids []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

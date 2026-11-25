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

package master

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/gorse-io/gorse/common/expression"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/logics"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// diversityHandler builds a routing container so go-restful populates path
// parameters exactly as in production.
func (s *MasterTestSuite) diversityHandler() http.Handler {
	ws := new(restful.WebService)
	ws.Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
	ws.Path("/api/")
	ws.Route(ws.GET("/dashboard/recommend/{user-id}").To(s.getRecommend))
	container := restful.NewContainer()
	container.Add(ws)
	return container
}

// newDiversityRequest sends a routed GET recommendation request.
func (s *MasterTestSuite) newDiversityRequest(userId string, n int) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet,
		"/api/dashboard/recommend/"+userId+"?n="+strconv.Itoa(n), nil)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	s.diversityHandler().ServeHTTP(recorder, req)
	return recorder
}

// TestServingDiversityRevalidationAndDegradation verifies the serving side
// re-constrains an unconstrained generation artifact, flags degradation, and
// exposes the folding/relaxation report through an additive header.
func (s *MasterTestSuite) TestServingDiversityRevalidationAndDegradation() {
	ctx := s.T().Context()
	s.Config.Recommend.Ranker.Type = "none"
	s.Config.Recommend.Ranker.Recommenders = []string{"latest"}
	s.Config.Recommend.DataSource.PositiveFeedbackTypes = []expression.FeedbackTypeExpression{
		{FeedbackType: "read"},
	}
	s.Config.Recommend.Diversity = config.DiversityConfig{
		DedupKey:   "item.Labels.series_id",
		MultiLabel: config.MultiLabelAll,
		Categories: []config.CategoryBudget{{Name: "movie", MaxCount: 2, MaxConsecutive: 1}},
	}
	// Four items in two series, all movie; one music minority item.
	now := time.Unix(1_700_000_000, 0).UTC()
	items := []data.Item{
		{ItemId: "m0a", Categories: []string{"movie"}, Timestamp: now, Labels: map[string]any{"series_id": "s1"}},
		{ItemId: "m0b", Categories: []string{"movie"}, Timestamp: now, Labels: map[string]any{"series_id": "s1"}},
		{ItemId: "m1a", Categories: []string{"movie"}, Timestamp: now, Labels: map[string]any{"series_id": "s2"}},
		{ItemId: "m1b", Categories: []string{"movie"}, Timestamp: now, Labels: map[string]any{"series_id": "s2"}},
		{ItemId: "u0", Categories: []string{"music"}, Timestamp: now, Labels: map[string]any{"series_id": "s3"}},
	}
	require.NoError(s.T(), s.DataClient.BatchInsertItems(ctx, items))
	require.NoError(s.T(), s.DataClient.BatchInsertUsers(ctx, []data.User{{UserId: "erin"}}))

	// No generation artifact exists: the serving side must re-constrain and
	// mark the response degraded rather than trust an unconstrained list.
	recorder := s.newDiversityRequest("erin", 5)
	require.Equal(s.T(), 200, recorder.Code)
	header := recorder.Header().Get(logics.ConstraintReportHeader)
	require.NotEmpty(s.T(), header)
	var report logics.ConstraintReport
	require.NoError(s.T(), json.Unmarshal([]byte(header), &report))
	assert.True(s.T(), report.Degraded, "missing generation artifact must be flagged as degraded")
	assert.Greater(s.T(), report.Folded, 0)
	// The movie hard cap holds.
	movieCount := 0
	for _, stat := range report.CategoryStats {
		if stat.Category == "movie" {
			movieCount = stat.Count
		}
	}
	assert.LessOrEqual(s.T(), movieCount, 2)

	// Now write a generation artifact matching the current configuration.
	matched := logics.ConstraintReport{ConfigHash: s.Config.Recommend.Diversity.Hash()}
	bytes, err := json.Marshal(matched)
	require.NoError(s.T(), err)
	require.NoError(s.T(), s.CacheClient.Set(ctx, cache.String(
		cache.Key(cache.RecommendConstraintReport, "erin"), string(bytes))))
	recorder2 := s.newDiversityRequest("erin", 5)
	require.Equal(s.T(), 200, recorder2.Code)
	var report2 logics.ConstraintReport
	require.NoError(s.T(), json.Unmarshal([]byte(recorder2.Header().Get(logics.ConstraintReportHeader)), &report2))
	assert.False(s.T(), report2.Degraded, "a matching artifact must not be flagged as degraded")
}

// TestServingDiversityDisabledHasNoHeader verifies the unconfigured path keeps
// the legacy response with no additive constraint header at all.
func (s *MasterTestSuite) TestServingDiversityDisabledHasNoHeader() {
	ctx := s.T().Context()
	s.Config.Recommend.Ranker.Type = "none"
	s.Config.Recommend.Ranker.Recommenders = []string{"latest"}
	require.False(s.T(), s.Config.Recommend.Diversity.Enabled())
	now := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(s.T(), s.DataClient.BatchInsertItems(ctx, []data.Item{
		{ItemId: "legacy-1", Timestamp: now},
		{ItemId: "legacy-2", Timestamp: now},
	}))
	recorder := s.newDiversityRequest("frank", 2)
	require.Equal(s.T(), 200, recorder.Code)
	assert.Empty(s.T(), recorder.Header().Get(logics.ConstraintReportHeader))
}

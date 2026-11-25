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
	"testing"

	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func diversityItems() map[string]data.Item {
	return map[string]data.Item{
		"a1": {ItemId: "a1", Categories: []string{"movie"}, Labels: map[string]any{"series": "s1", "kind": "trailer"}},
		"a2": {ItemId: "a2", Categories: []string{"movie"}, Labels: map[string]any{"series": "s1", "kind": "full"}},
		"a3": {ItemId: "a3", Categories: []string{"movie"}, Labels: map[string]any{"series": "s1", "kind": "behind"}},
		"b1": {ItemId: "b1", Categories: []string{"music"}, Labels: map[string]any{"series": "s2"}},
		"b2": {ItemId: "b2", Categories: []string{"music"}, Labels: map[string]any{"series": "s2"}},
		"c1": {ItemId: "c1", Categories: []string{"sport"}, Labels: map[string]any{"series": "s3"}},
		"d1": {ItemId: "d1", Categories: []string{"movie", "music"}, Labels: map[string]any{"series": "s4"}},
		"e1": {ItemId: "e1", Labels: map[string]any{"series": "s5"}},
	}
}

func scoresFrom(ids ...string) []cache.Score {
	scores := make([]cache.Score, len(ids))
	for i, id := range ids {
		// Descending distinct scores: earlier candidates rank higher.
		scores[i] = cache.Score{Id: id, Score: float64(100 - i)}
	}
	return scores
}

func idsOf(scores []cache.Score) []string {
	ids := make([]string, len(scores))
	for i, score := range scores {
		ids[i] = score.Id
	}
	return ids
}

// TestDiversityFoldingAndRepresentative verifies group folding, highest-score
// representative selection and stable tie-breaking.
func TestDiversityFoldingAndRepresentative(t *testing.T) {
	engine, err := NewConstraintEngine(config.DiversityConfig{
		DedupKey:   "item.Labels.series",
		MultiLabel: config.MultiLabelAll,
	})
	require.NoError(t, err)
	require.NotNil(t, engine)

	// a2 out-scores a1 and a3 within group s1.
	scores := []cache.Score{
		{Id: "a1", Score: 1},
		{Id: "a2", Score: 9},
		{Id: "a3", Score: 5},
		{Id: "b1", Score: 8},
		{Id: "b2", Score: 7},
		{Id: "c1", Score: 6},
	}
	out, report := engine.Apply(scores, diversityItems(), nil, 0)
	assert.Equal(t, []string{"a2", "b1", "c1"}, idsOf(out))
	assert.Equal(t, 3, report.Folded)
	assert.Equal(t, 3, report.Returned)
	assert.False(t, report.Exhausted)
	assert.Equal(t, RelaxLevelNone, report.RelaxLevel)
}

// TestDiversityTieBreak verifies that equal scores keep the stable incoming
// order and repeated runs are item-by-item identical.
func TestDiversityTieBreak(t *testing.T) {
	engine, err := NewConstraintEngine(config.DiversityConfig{
		DedupKey:   "item.Labels.series",
		MultiLabel: config.MultiLabelAll,
	})
	require.NoError(t, err)
	scores := []cache.Score{
		{Id: "a1", Score: 5},
		{Id: "a2", Score: 5},
		{Id: "a3", Score: 5},
		{Id: "b1", Score: 5},
	}
	first, report1 := engine.Apply(scores, diversityItems(), nil, 0)
	second, report2 := engine.Apply(scores, diversityItems(), nil, 0)
	assert.Equal(t, []string{"a1", "b1"}, idsOf(first))
	assert.Equal(t, idsOf(first), idsOf(second))
	assert.Equal(t, report1.Folded, report2.Folded)
	// A fresh engine over the same inputs yields the same representatives.
	engine2, err := NewConstraintEngine(config.DiversityConfig{
		DedupKey:   "item.Labels.series",
		MultiLabel: config.MultiLabelAll,
	})
	require.NoError(t, err)
	third, _ := engine2.Apply(scores, diversityItems(), nil, 0)
	assert.Equal(t, idsOf(first), idsOf(third))
}

// TestDiversityConsumedDiffusion verifies that consuming any group member
// removes the whole group, and the representative never reappears afterwards.
func TestDiversityConsumedDiffusion(t *testing.T) {
	engine, err := NewConstraintEngine(config.DiversityConfig{
		DedupKey:   "item.Labels.series",
		MultiLabel: config.MultiLabelAll,
	})
	require.NoError(t, err)
	scores := scoresFrom("a1", "a2", "a3", "b1", "b2", "c1")
	consumed := map[string]struct{}{"a3": {}, "b1": {}}
	out, report := engine.Apply(scores, diversityItems(), consumed, 0)
	assert.Equal(t, []string{"c1"}, idsOf(out))
	// All five members of the two consumed groups were folded.
	assert.Equal(t, 5, report.Folded)
}

// TestDiversityKeyEvaluationFailure verifies that a failing key expression
// isolates the item under its own key and records a readable reason without
// failing the whole request.
func TestDiversityKeyEvaluationFailure(t *testing.T) {
	engine, err := NewConstraintEngine(config.DiversityConfig{
		// Missing field on every item: every key evaluation fails.
		DedupKey:   "item.Labels.missing.deep",
		MultiLabel: config.MultiLabelAll,
	})
	require.NoError(t, err)
	scores := scoresFrom("a1", "a2", "b1")
	out, report := engine.Apply(scores, diversityItems(), nil, 0)
	// Each item gets a unique fallback key, so nothing is folded.
	assert.Equal(t, []string{"a1", "a2", "b1"}, idsOf(out))
	assert.Equal(t, 3, report.KeyFailureCount)
	require.Len(t, report.KeyFailures, 3)
	for _, failure := range report.KeyFailures {
		assert.NotEmpty(t, failure.Reason)
	}
}

// TestDiversityCategoryBudgets builds a candidate set where one category
// occupies 80% and asserts count, ratio, consecutive caps, the fixed
// relaxation order and the exhaustion count/reason. The hard count cap is
// never silently lowered through: the request returns the actual feasible
// length with an explicit explanation.
func TestDiversityCategoryBudgets(t *testing.T) {
	items := map[string]data.Item{}
	scores := make([]cache.Score, 0, 10)
	// 8 movie items followed by 2 music items: movie share = 80%.
	for i := range 8 {
		id := "m" + string(rune('0'+i))
		items[id] = data.Item{ItemId: id, Categories: []string{"movie"}, Labels: map[string]any{"series": id}}
		scores = append(scores, cache.Score{Id: id, Score: float64(100 - i)})
	}
	for i := range 2 {
		id := "u" + string(rune('0'+i))
		items[id] = data.Item{ItemId: id, Categories: []string{"music"}, Labels: map[string]any{"series": id}}
		scores = append(scores, cache.Score{Id: id, Score: float64(20 - i)})
	}
	engine, err := NewConstraintEngine(config.DiversityConfig{
		DedupKey:   "item.Labels.series",
		MultiLabel: config.MultiLabelAll,
		Categories: []config.CategoryBudget{{
			Name:           "movie",
			MaxCount:       3,
			MaxRatio:       0.5,
			MaxConsecutive: 2,
		}},
	})
	require.NoError(t, err)
	out, report := engine.Apply(scores, items, nil, 10)
	// Feasible output: 3 movie (hard cap) + 2 music = 5 of 10 requested.
	assert.True(t, report.Exhausted)
	assert.NotEmpty(t, report.ExhaustedReason)
	assert.Equal(t, 5, len(out))
	assert.Equal(t, 10, report.Candidates)
	// No duplicates ever.
	assert.Len(t, idsOf(out), len(loUniq(idsOf(out))))
	// Movie count never exceeds its hard cap, even after relaxation.
	movieCount := 0
	musicCount := 0
	for _, score := range out {
		switch items[score.Id].Categories[0] {
		case "movie":
			movieCount++
		case "music":
			musicCount++
		}
	}
	assert.Equal(t, 3, movieCount)
	assert.Equal(t, 2, musicCount)
	// Consecutive and ratio caps had to be relaxed given the 80% movie pool;
	// the hard count cap did not.
	assert.GreaterOrEqual(t, report.RelaxLevel, RelaxLevelRatio)
	assert.Equal(t, 0, report.Relaxations[RelaxLevelName(RelaxLevelCount)])
	assert.GreaterOrEqual(t, report.Relaxations[RelaxLevelName(RelaxLevelRatio)], 1)
	// Category statistics match the actual output exactly.
	stats := map[string]CategoryStat{}
	for _, stat := range report.CategoryStats {
		stats[stat.Category] = stat
	}
	require.Contains(t, stats, "movie")
	require.Contains(t, stats, "music")
	assert.Equal(t, 3, stats["movie"].Count)
	assert.InDelta(t, 0.6, stats["movie"].Ratio, 1e-12)
	assert.Equal(t, 2, stats["music"].Count)
	assert.InDelta(t, 0.4, stats["music"].Ratio, 1e-12)
}

// TestDiversityRatioAndConsecutiveSatisfied verifies that when enough
// minority candidates exist, ratio and consecutive caps are met without any
// relaxation or exhaustion.
func TestDiversityRatioAndConsecutiveSatisfied(t *testing.T) {
	items := map[string]data.Item{}
	scores := make([]cache.Score, 0)
	// Interleaved pool of 4 movie and 4 music items.
	for i := range 4 {
		movieId := "m" + string(rune('0'+i))
		musicId := "u" + string(rune('0'+i))
		items[movieId] = data.Item{ItemId: movieId, Categories: []string{"movie"}, Labels: map[string]any{"series": movieId}}
		items[musicId] = data.Item{ItemId: musicId, Categories: []string{"music"}, Labels: map[string]any{"series": musicId}}
		scores = append(scores,
			cache.Score{Id: movieId, Score: float64(100 - 2*i)},
			cache.Score{Id: musicId, Score: float64(99 - 2*i)})
	}
	engine, err := NewConstraintEngine(config.DiversityConfig{
		DedupKey:   "item.Labels.series",
		MultiLabel: config.MultiLabelAll,
		Categories: []config.CategoryBudget{{
			Name:           "movie",
			MaxCount:       4,
			MaxRatio:       0.5,
			MaxConsecutive: 1,
		}},
	})
	require.NoError(t, err)
	out, report := engine.Apply(scores, items, nil, 8)
	assert.Equal(t, 8, len(out))
	assert.False(t, report.Exhausted)
	assert.Equal(t, RelaxLevelNone, report.RelaxLevel)
	// No two movies adjacent.
	previousMovie := false
	for _, score := range out {
		isMovie := items[score.Id].Categories[0] == "movie"
		if isMovie {
			assert.False(t, previousMovie, "consecutive movie cap violated at %s", score.Id)
		}
		previousMovie = isMovie
	}
}

// TestDiversityRelaxationOrder verifies the fixed consecutive -> ratio
// relaxation order. The count cap is a hard ceiling: once reached the pool
// exhausts instead of lowering the cap silently.
func TestDiversityRelaxationOrder(t *testing.T) {
	items := map[string]data.Item{}
	scores := make([]cache.Score, 0, 4)
	for i := range 4 {
		id := "m" + string(rune('0'+i))
		items[id] = data.Item{ItemId: id, Categories: []string{"movie"}, Labels: map[string]any{"series": id}}
		scores = append(scores, cache.Score{Id: id, Score: float64(10 - i)})
	}
	engine, err := NewConstraintEngine(config.DiversityConfig{
		DedupKey:   "item.Labels.series",
		MultiLabel: config.MultiLabelAll,
		Categories: []config.CategoryBudget{{
			Name:           "movie",
			MaxCount:       2,
			MaxRatio:       0.1,
			MaxConsecutive: 1,
		}},
	})
	require.NoError(t, err)
	out, report := engine.Apply(scores, items, nil, 4)
	// Consecutive and ratio are relaxed in order (effective level 2 = ratio);
	// afterwards the hard count cap stops the sequence at 2 of 4 items.
	assert.Equal(t, 2, len(out))
	assert.True(t, report.Exhausted)
	assert.Equal(t, RelaxLevelRatio, report.RelaxLevel)
	assert.Equal(t, 1, report.Relaxations[RelaxLevelName(RelaxLevelConsecutive)])
	assert.Equal(t, 1, report.Relaxations[RelaxLevelName(RelaxLevelRatio)])
	assert.Equal(t, 0, report.Relaxations[RelaxLevelName(RelaxLevelCount)])
	// Only the two highest-scoring representatives survive.
	assert.Equal(t, []string{"m0", "m1"}, idsOf(out))
}

// TestDiversityMultiLabelPrimary verifies that primary counting never charges
// one item against several budgets at once.
func TestDiversityMultiLabelPrimary(t *testing.T) {
	items := map[string]data.Item{
		"x": {ItemId: "x", Categories: []string{"movie", "music"}, Labels: map[string]any{"series": "x"}},
		"y": {ItemId: "y", Categories: []string{"music"}, Labels: map[string]any{"series": "y"}},
	}
	cfg := config.DiversityConfig{
		DedupKey:   "item.Labels.series",
		MultiLabel: config.MultiLabelPrimary,
		Categories: []config.CategoryBudget{
			{Name: "movie", MaxCount: 1, MaxConsecutive: 1},
			{Name: "music", MaxCount: 1, MaxConsecutive: 1},
		},
	}
	engine, err := NewConstraintEngine(cfg)
	require.NoError(t, err)
	out, report := engine.Apply(
		[]cache.Score{{Id: "x", Score: 2}, {Id: "y", Score: 1}},
		items, nil, 2,
	)
	// x counts only against movie, y only against music: both fit.
	assert.Equal(t, []string{"x", "y"}, idsOf(out))
	assert.False(t, report.Exhausted)
}

// TestDiversityDisabledIsPassthrough verifies the unconfigured path returns
// the input untouched and no engine is constructed.
func TestDiversityDisabledIsPassthrough(t *testing.T) {
	engine, err := NewConstraintEngine(config.DiversityConfig{})
	require.NoError(t, err)
	assert.Nil(t, engine)
	assert.False(t, config.DiversityConfig{}.Enabled())
}

// TestDiversityDeterministicUnderReorder verifies representatives depend on
// score and stable order, not map iteration.
func TestDiversityDeterministicUnderReorder(t *testing.T) {
	cfg := config.DiversityConfig{DedupKey: "item.Labels.series", MultiLabel: config.MultiLabelAll}
	engine, err := NewConstraintEngine(cfg)
	require.NoError(t, err)
	scores := []cache.Score{
		{Id: "b2", Score: 9}, {Id: "b1", Score: 9},
		{Id: "a3", Score: 9}, {Id: "a2", Score: 9}, {Id: "a1", Score: 9},
		{Id: "c1", Score: 9},
	}
	out, _ := engine.Apply(scores, diversityItems(), nil, 0)
	assert.Equal(t, []string{"b2", "a3", "c1"}, idsOf(out))
}

func loUniq(ids []string) []string {
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

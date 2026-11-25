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
	"sort"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/gorse-io/gorse/common/log"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// Relaxation levels, ordered from the strictest effective policy to the
// loosest. The fixed relaxation order required by the specification is:
// consecutive constraint -> ratio constraint -> count constraint. Reaching
// the count level means the count cap is the only budget still enforced; the
// hard count cap itself is never silently lowered, so an infeasible pool
// exhausts with the actual feasible length instead of being filled.
const (
	RelaxLevelNone = iota
	RelaxLevelConsecutive
	RelaxLevelRatio
	RelaxLevelCount
)

// RelaxLevelName is the human-readable name of the effective relaxation level.
func RelaxLevelName(level int) string {
	switch level {
	case RelaxLevelNone:
		return "none"
	case RelaxLevelConsecutive:
		return "consecutive"
	case RelaxLevelRatio:
		return "ratio"
	case RelaxLevelCount:
		return "count"
	default:
		return "unknown"
	}
}

// KeyEvaluationFailure records a de-duplication key that could not be evaluated.
// It never carries raw label or comment text, only the item identifier and a
// machine-readable reason.
type KeyEvaluationFailure struct {
	ItemId string `json:"item_id"`
	Reason string `json:"reason"`
}

// CategoryStat is the observed output share of a category.
type CategoryStat struct {
	Category string  `json:"category"`
	Count    int     `json:"count"`
	Ratio    float64 `json:"ratio"`
}

// ConstraintReport explains how a candidate list was folded and relaxed.
// All fields are safe to expose: they contain only counts, category names,
// configuration-derived expressions and item identifiers.
type ConstraintReport struct {
	// ConfigHash is the hash of the diversity configuration in effect.
	ConfigHash string `json:"config_hash"`
	// Candidates is the number of candidates entering the engine.
	Candidates int `json:"candidates"`
	// Folded is the number of candidates removed by group de-duplication
	// (including representatives of already-consumed groups).
	Folded int `json:"folded"`
	// Returned is the number of items emitted.
	Returned int `json:"returned"`
	// Exhausted is true when fewer than the requested limit could be produced.
	Exhausted bool `json:"exhausted"`
	// ExhaustedReason explains why the candidate pool ran dry.
	ExhaustedReason string `json:"exhausted_reason,omitempty"`
	// RelaxLevel is the highest relaxation level that had to be applied
	// (0 = none, 1 = consecutive, 2 = ratio, 3 = count).
	RelaxLevel int `json:"relax_level"`
	// Relaxations counts how often each level was applied.
	Relaxations map[string]int `json:"relaxations"`
	// KeyFailures records items whose de-duplication key failed to evaluate.
	KeyFailures []KeyEvaluationFailure `json:"key_failures,omitempty"`
	// KeyFailureCount is the number of affected items.
	KeyFailureCount int `json:"key_failure_count"`
	// CategoryStats is the per-category share of the emitted sequence.
	CategoryStats []CategoryStat `json:"category_stats"`
	// Degraded is set on the serving side when the stored generation artifact
	// is missing or was produced under a different configuration.
	Degraded bool `json:"degraded,omitempty"`
}

// ConstraintEngine is the single implementation of de-duplication key
// resolution, representative selection and diversity budget enforcement.
// Both the generation pipeline and the online serving path construct it from
// the same configuration source; there is no second implementation anywhere.
type ConstraintEngine struct {
	cfg       config.DiversityConfig
	keyFunc   *vm.Program
	keyFailed map[string]string
}

// NewConstraintEngine compiles the de-duplication key expression. A nil engine
// is returned (without error) when no structural constraint is configured, so
// callers keep the legacy behavior.
func NewConstraintEngine(cfg config.DiversityConfig) (*ConstraintEngine, error) {
	if !cfg.Enabled() {
		return nil, nil
	}
	if cfg.MultiLabel == "" {
		cfg.MultiLabel = config.MultiLabelAll
	}
	engine := &ConstraintEngine{
		cfg:       cfg,
		keyFailed: make(map[string]string),
	}
	if cfg.DedupKey != "" {
		program, err := expr.Compile(cfg.DedupKey, expr.Env(map[string]any{
			"item": data.Item{},
		}))
		if err != nil {
			return nil, fmt.Errorf("invalid diversity.dedup_key expression: %w", err)
		}
		engine.keyFunc = program
	}
	return engine, nil
}

// resolveKey evaluates the de-duplication group key of an item. Evaluation
// failures isolate the item under a unique key instead of failing the request.
func (e *ConstraintEngine) resolveKey(item data.Item) string {
	if e.keyFunc == nil {
		// No key expression: every item is its own group.
		return "\x00item\x00" + item.ItemId
	}
	result, err := expr.Run(e.keyFunc, map[string]any{"item": item})
	if err != nil {
		e.keyFailed[item.ItemId] = "evaluation_error"
		return "\x00failed\x00" + item.ItemId
	}
	if result == nil {
		e.keyFailed[item.ItemId] = "nil_key"
		return "\x00failed\x00" + item.ItemId
	}
	key, ok := result.(string)
	if !ok {
		e.keyFailed[item.ItemId] = "non_string_key"
		return "\x00failed\x00" + item.ItemId
	}
	return key
}

// ResolveGroupKey evaluates the de-duplication group key of an item. It is
// used by the generation side to pre-resolve consumed groups whose members
// were filtered out before the engine received the candidate list.
func (e *ConstraintEngine) ResolveGroupKey(item data.Item) string {
	if e == nil {
		return "\x00item\x00" + item.ItemId
	}
	return e.resolveKey(item)
}

// countingCategories returns the categories against whose budgets an item is
// counted. In "primary" mode only the first category is used, so a
// multi-category item never consumes several budgets at once.
func (e *ConstraintEngine) countingCategories(item data.Item) []string {
	if len(item.Categories) == 0 {
		return nil
	}
	if e.cfg.MultiLabel == config.MultiLabelPrimary {
		return item.Categories[:1]
	}
	return item.Categories
}

type constraintCandidate struct {
	score cache.Score
	item  data.Item
	index int
	key   string
}

type budgetState struct {
	count       int
	consecutive int
}

// Apply folds and constrains a candidate sequence.
//
//   - scores: candidates in their incoming (ranked) order; the incoming order
//     is the stable tie-break order, so repeated runs are item-by-item
//     identical for identical inputs.
//   - items: item attributes keyed by item id, used for key and category
//     resolution. Missing items keep their own group and score categories.
//   - consumedItemIds: items read or clicked by the user. Any group containing
//     a consumed item is treated as consumed as a whole (group diffusion).
//   - consumedGroups: group keys already known to be consumed. The generation
//     side filters consumed members before the engine runs, so their keys are
//     passed explicitly to preserve group diffusion across the missing members.
//   - limit: requested output length. A non-positive limit means all
//     surviving candidates.
func (e *ConstraintEngine) Apply(
	scores []cache.Score,
	items map[string]data.Item,
	consumedItemIds map[string]struct{},
	limit int,
) ([]cache.Score, *ConstraintReport) {
	return e.ApplyWithConsumedGroups(scores, items, consumedItemIds, nil, limit)
}

// ApplyWithConsumedGroups is Apply with pre-resolved consumed group keys.
func (e *ConstraintEngine) ApplyWithConsumedGroups(
	scores []cache.Score,
	items map[string]data.Item,
	consumedItemIds map[string]struct{},
	consumedGroupKeys map[string]struct{},
	limit int,
) ([]cache.Score, *ConstraintReport) {
	report := &ConstraintReport{
		ConfigHash:  e.cfg.Hash(),
		Candidates:  len(scores),
		Relaxations: map[string]int{RelaxLevelName(RelaxLevelConsecutive): 0, RelaxLevelName(RelaxLevelRatio): 0, RelaxLevelName(RelaxLevelCount): 0},
	}
	if e == nil {
		// Defensive: callers normally skip the engine entirely when disabled.
		out := append([]cache.Score(nil), scores...)
		if limit > 0 && len(out) > limit {
			out = out[:limit]
		}
		report.Returned = len(out)
		return out, report
	}

	// 1. Resolve group keys and diffuse the consumed state over groups.
	candidates := make([]constraintCandidate, 0, len(scores))
	consumedGroups := make(map[string]struct{})
	for key := range consumedGroupKeys {
		consumedGroups[key] = struct{}{}
	}
	for i, score := range scores {
		item, ok := items[score.Id]
		if !ok {
			item = data.Item{ItemId: score.Id, Categories: score.Categories}
		}
		key := e.resolveKey(item)
		if _, consumed := consumedItemIds[score.Id]; consumed {
			consumedGroups[key] = struct{}{}
		}
		candidates = append(candidates, constraintCandidate{
			score: score,
			item:  item,
			index: i,
			key:   key,
		})
	}

	// 2. Group de-duplication: one representative per group, highest score
	// wins, ties broken by the stable incoming order. Groups containing a
	// consumed item are removed entirely.
	representatives := make(map[string]constraintCandidate)
	groupOrder := make([]string, 0)
	folded := 0
	for _, candidate := range candidates {
		if _, consumed := consumedGroups[candidate.key]; consumed {
			folded++
			continue
		}
		incumbent, exists := representatives[candidate.key]
		if !exists {
			representatives[candidate.key] = candidate
			groupOrder = append(groupOrder, candidate.key)
			continue
		}
		folded++
		if higherScore(candidate.score.Score, incumbent.score.Score) ||
			(candidate.score.Score == incumbent.score.Score && candidate.index < incumbent.index) {
			representatives[candidate.key] = candidate
		}
	}
	survivors := make([]constraintCandidate, 0, len(groupOrder))
	for _, key := range groupOrder {
		survivors = append(survivors, representatives[key])
	}
	// Representatives inherit the representative's incoming position, keeping
	// the output deterministic and as close to the ranked order as possible.
	sort.SliceStable(survivors, func(i, j int) bool {
		return survivors[i].index < survivors[j].index
	})

	// 3. Budget enforcement with the fixed relaxation order.
	budgets := make(map[string]*budgetState)
	getBudget := func(category string) *budgetState {
		state, ok := budgets[category]
		if !ok {
			state = &budgetState{}
			budgets[category] = state
		}
		return state
	}
	emitted := make([]cache.Score, 0, len(survivors))
	blocked := make(map[string]int)
	effectiveLevel := RelaxLevelNone
	// The level only moves upward; once a level is relaxed it stays relaxed.
	currentLevel := RelaxLevelNone
	// admittedAtLevel records that at least one item was admitted while a
	// given level was relaxed, so relaxation is counted only when it actually
	// changed the output rather than when a candidate was merely deferred.
	admittedAtLevel := map[int]bool{}

	withinBudget := func(candidate constraintCandidate, level int) (bool, string) {
		categories := e.countingCategories(candidate.item)
		if len(categories) == 0 {
			return true, ""
		}
		for _, category := range categories {
			budgetCfg, configured := e.cfg.CategoryBudgetConfig(category)
			state := getBudget(category)
			// Count cap: the hard ceiling. It is enforced at every relaxation
			// level so the budget is never silently lowered to fill a page.
			if configured && budgetCfg.MaxCount > 0 &&
				state.count >= budgetCfg.MaxCount {
				return false, "count"
			}
			// Ratio cap, evaluated against the length the output would have
			// after admitting this item.
			if level < RelaxLevelRatio && configured && budgetCfg.MaxRatio > 0 {
				projectedTotal := len(emitted) + 1
				if float64(state.count+1)/float64(projectedTotal) > budgetCfg.MaxRatio+1e-12 {
					return false, "ratio"
				}
			}
			// Consecutive cap.
			if level < RelaxLevelConsecutive && configured && budgetCfg.MaxConsecutive > 0 &&
				state.consecutive >= budgetCfg.MaxConsecutive {
				return false, "consecutive"
			}
		}
		return true, ""
	}

	admit := func(candidate constraintCandidate) {
		emitted = append(emitted, candidate.score)
		// Reset consecutive runs of every other category, advance the runs of
		// the counting categories.
		for category := range budgets {
			budgets[category].consecutive = 0
		}
		for _, category := range e.countingCategories(candidate.item) {
			state := getBudget(category)
			state.count++
			state.consecutive++
		}
	}

	// Multi-pass scheduling with a fixed relaxation ladder. Each pass scans
	// only the candidates already in hand (never the full library). When a
	// pass admits nothing and the blockers are soft constraints, the ladder
	// climbs exactly one rung and the same pool is re-scanned. A level is
	// counted as relaxed only when an item is actually admitted under it, so
	// candidates merely deferred to a later strict pass do not inflate the
	// relaxation counters. The hard count cap is enforced at every rung.
	pending := append([]constraintCandidate(nil), survivors...)
	for len(pending) > 0 && (limit <= 0 || len(emitted) < limit) {
		progressed := false
		next := make([]constraintCandidate, 0, len(pending))
		passBlocked := map[string]int{}
		for _, candidate := range pending {
			if limit > 0 && len(emitted) >= limit {
				next = append(next, candidate)
				continue
			}
			ok, violated := withinBudget(candidate, currentLevel)
			if ok {
				if currentLevel > RelaxLevelNone && !admittedAtLevel[currentLevel] {
					admittedAtLevel[currentLevel] = true
					report.Relaxations[RelaxLevelName(currentLevel)]++
					effectiveLevel = max(effectiveLevel, currentLevel)
				}
				admit(candidate)
				progressed = true
				continue
			}
			// The candidate is blocked at the current level. It may become
			// admissible later as other categories fill the sequence, so it
			// stays pending; blocked[] only feeds the exhaustion explanation.
			blocked[violated]++
			passBlocked[violated]++
			next = append(next, candidate)
		}
		pending = next
		if limit > 0 && len(emitted) >= limit {
			break
		}
		if progressed {
			// Re-scan the remaining pool at the same strictness: a deferred
			// candidate may now fit as other categories filled the sequence.
			// Blocks that resolve on a later strict pass are not relaxations.
			continue
		}
		// A full pass admitted nothing. If only the hard count cap is
		// blocking, no softer relaxation can help: exhaust at the feasible
		// length without ever lowering the count cap.
		if passBlocked["ratio"] == 0 && passBlocked["consecutive"] == 0 {
			break
		}
		if currentLevel >= RelaxLevelCount {
			break
		}
		// Climb exactly one rung of the fixed ladder and re-scan. Every rung
		// is visited in order, so the effective level faithfully reports how
		// far the policy was loosened. A rung is counted once an item is
		// actually admitted under it; a rung that admits nothing is still
		// recorded as reached because the strict constraint was loosened.
		currentLevel++
		if !admittedAtLevel[currentLevel] {
			admittedAtLevel[currentLevel] = true
			report.Relaxations[RelaxLevelName(currentLevel)]++
			effectiveLevel = max(effectiveLevel, currentLevel)
		}
	}

	// 4. Assemble the report.
	report.Folded = folded
	report.Returned = len(emitted)
	report.RelaxLevel = effectiveLevel
	for itemId, reason := range e.keyFailed {
		report.KeyFailures = append(report.KeyFailures, KeyEvaluationFailure{ItemId: itemId, Reason: reason})
	}
	sort.Slice(report.KeyFailures, func(i, j int) bool {
		return report.KeyFailures[i].ItemId < report.KeyFailures[j].ItemId
	})
	report.KeyFailureCount = len(report.KeyFailures)
	categoryCount := make(map[string]int)
	for _, score := range emitted {
		item, ok := items[score.Id]
		if !ok {
			item = data.Item{Categories: score.Categories}
		}
		for _, category := range e.countingCategories(item) {
			categoryCount[category]++
		}
	}
	report.CategoryStats = lo.MapToSlice(categoryCount, func(category string, count int) CategoryStat {
		ratio := 0.0
		if len(emitted) > 0 {
			ratio = float64(count) / float64(len(emitted))
		}
		return CategoryStat{Category: category, Count: count, Ratio: ratio}
	})
	sort.Slice(report.CategoryStats, func(i, j int) bool {
		return report.CategoryStats[i].Category < report.CategoryStats[j].Category
	})
	if limit > 0 && len(emitted) < limit {
		report.Exhausted = true
		switch {
		case len(blocked) > 0:
			report.ExhaustedReason = fmt.Sprintf(
				"only %d of %d requested items available: %d candidates blocked by diversity budgets (count=%d, ratio=%d, consecutive=%d), %d folded by de-duplication",
				len(emitted), limit,
				blocked["count"]+blocked["ratio"]+blocked["consecutive"],
				blocked["count"], blocked["ratio"], blocked["consecutive"], folded)
		case folded > 0:
			report.ExhaustedReason = fmt.Sprintf(
				"only %d of %d requested items available: %d candidates folded by de-duplication",
				len(emitted), limit, folded)
		default:
			report.ExhaustedReason = fmt.Sprintf(
				"only %d of %d requested items available: candidate pool exhausted",
				len(emitted), limit)
		}
		log.Logger().Debug("diversity constraint exhausted the candidate pool",
			zap.String("reason", report.ExhaustedReason))
	}
	return emitted, report
}

// higherScore keeps the representative rule explicit and NaN-safe: NaN scores
// never replace an incumbent.
func higherScore(candidate, incumbent float64) bool {
	return candidate == candidate && candidate > incumbent
}

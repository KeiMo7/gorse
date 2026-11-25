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
	"context"
	"fmt"
	"strings"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gorse-io/gorse/common/expression"
	"github.com/gorse-io/gorse/common/heap"
	"github.com/gorse-io/gorse/common/log"
	"github.com/gorse-io/gorse/common/util"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/storage"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/gorse-io/gorse/storage/vectors"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

const (
	LatestRecommender          = "latest"
	NonPersonalizedRecommender = "non-personalized/"
	ItemToItemRecommender      = "item-to-item/"
	UserToUserRecommender      = "user-to-user/"
	ExternalRecommender        = "external/"
	CollaborativeRecommender   = "collaborative"
)

type Recommender struct {
	config       config.RecommendConfig
	cacheClient  cache.Database
	dataClient   data.Database
	vectorClient vectors.Database

	online       bool
	coldstart    bool
	userId       string
	userFeedback []data.Feedback
	categories   []string
	excludeSet   mapset.Set[string]

	// LastConstraintReport is the folding/relaxation report of the most recent
	// Recommend call. It is nil when the structural constraint is disabled.
	LastConstraintReport *ConstraintReport

	// skipStructuralConstraint lets the serving side take over constraint
	// enforcement itself, so the engine never runs twice on one request.
	skipStructuralConstraint bool
}

type RecommenderFunc func(ctx context.Context) ([]cache.Score, string, error)

// ConstraintReportHeader carries the JSON-serialized ConstraintReport through
// an HTTP response without changing the response body, so legacy clients keep
// byte-identical payloads.
const ConstraintReportHeader = "X-Gorse-Constraint-Report"

func NewRecommender(config config.RecommendConfig, cacheClient cache.Database, dataClient data.Database, vectorClient vectors.Database, online bool, userId string, categories []string) (*Recommender, error) {
	// Load user feedback
	userFeedback, err := dataClient.GetUserFeedback(context.Background(), userId, new(time.Now()))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	excludeSet := mapset.NewSet[string]()
	coldstart := true
	for _, feedback := range userFeedback {
		// Negative feedback items should always be excluded (highest priority)
		if expression.MatchFeedbackTypeExpressions(config.DataSource.NegativeFeedbackTypes, feedback.FeedbackType, feedback.Value) {
			excludeSet.Add(feedback.ItemId)
		} else if !config.Replacement.EnableReplacement || !online {
			// Other feedback items are excluded unless replacement is enabled
			excludeSet.Add(feedback.ItemId)
		}
		if expression.MatchFeedbackTypeExpressions(config.DataSource.PositiveFeedbackTypes, feedback.FeedbackType, feedback.Value) {
			coldstart = false
		}
	}
	return &Recommender{
		config:       config,
		cacheClient:  cacheClient,
		dataClient:   dataClient,
		vectorClient: vectorClient,
		userId:       userId,
		userFeedback: userFeedback,
		online:       online,
		coldstart:    coldstart,
		categories:   categories,
		excludeSet:   excludeSet,
	}, nil
}

func (r *Recommender) ExcludeSet() mapset.Set[string] {
	return r.excludeSet
}

func (r *Recommender) UserFeedback() []data.Feedback {
	return r.userFeedback
}

func (r *Recommender) IsColdStart() bool {
	return r.coldstart
}

func (r *Recommender) Recommend(ctx context.Context, limit int) (result []cache.Score, err error) {
	if !strings.EqualFold(r.config.Ranker.Type, "none") {
		scores, err := r.cacheClient.SearchScores(ctx, cache.Recommend, r.userId, r.categories, 0, r.config.CacheSize)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		result = make([]cache.Score, 0, len(scores))
		for _, score := range scores {
			if !r.excludeSet.Contains(score.Id) {
				r.excludeSet.Add(score.Id)
				result = append(result, score)
			}
		}
	} else {
		result, _, err = r.RecommendSequential(ctx, result, r.config.CacheSize, r.config.Ranker.Recommenders...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	// When structural constraints are enabled, always gather the full
	// candidate window (bounded by CacheSize, never a full-library scan) so
	// budgets and folding see every available candidate regardless of the
	// requested page size. Without constraints the legacy short-circuit is
	// preserved exactly.
	fetchLimit := limit
	if r.config.Diversity.Enabled() {
		fetchLimit = r.config.CacheSize
	}
	if len(result) >= fetchLimit && fetchLimit > 0 {
		result = result[:fetchLimit]
	} else {
		result, _, err = r.RecommendSequential(ctx, result, fetchLimit, r.config.Fallback.Recommenders...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	// Structural de-duplication and diversity budgets are the single final
	// stage shared by every link (main ranking, fallback, replacements and
	// external sources have already been merged above). On the serving side
	// the caller re-runs the same engine with fresh consumed-state and
	// degradation handling, so it is skipped here to avoid double folding.
	if r.skipStructuralConstraint {
		if limit > 0 && len(result) > limit {
			return result[:limit], nil
		}
		return result, nil
	}
	// The engine enforces budgets over the full request position and records
	// exhaustion when the feasible output is shorter than the request.
	constrained, err := r.applyConstraints(ctx, result, limit)
	if err != nil {
		return nil, err
	}
	if limit > 0 && len(constrained) > limit {
		return constrained[:limit], nil
	}
	return constrained, nil
}

// SkipStructuralConstraint marks this recommender as externally constrained.
// The serving-side re-validation owns the single engine invocation then.
func (r *Recommender) SkipStructuralConstraint() {
	r.skipStructuralConstraint = true
}

// applyConstraints runs the single constraint engine over the merged
// candidates. When no de-duplication key and no budget are configured it is a
// pure pass-through, preserving the legacy output item by item.
func (r *Recommender) applyConstraints(ctx context.Context, result []cache.Score, limit int) ([]cache.Score, error) {
	r.LastConstraintReport = nil
	if !r.config.Diversity.Enabled() {
		if limit > 0 && len(result) > limit {
			return result[:limit], nil
		}
		return result, nil
	}
	engine, err := NewConstraintEngine(r.config.Diversity)
	if err != nil {
		// An invalid expression must not have survived config loading, but if
		// it does, fail open to the unconstrained list rather than the request.
		log.Logger().Error("failed to compile diversity constraint, returning unconstrained result", zap.Error(err))
		if limit > 0 && len(result) > limit {
			return result[:limit], nil
		}
		return result, nil
	}
	if engine == nil {
		if limit > 0 && len(result) > limit {
			return result[:limit], nil
		}
		return result, nil
	}
	items, err := r.LoadConstraintItems(ctx, result)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	consumed := make(map[string]struct{})
	consumedIds := make([]string, 0)
	for _, feedback := range r.userFeedback {
		consumed[feedback.ItemId] = struct{}{}
		consumedIds = append(consumedIds, feedback.ItemId)
	}
	// Resolve consumed group keys explicitly: excludeSet already removed the
	// consumed members, but group diffusion must still fold the surviving
	// representatives of the same group.
	consumedGroupKeys := make(map[string]struct{})
	consumedRecords, err := r.dataClient.BatchGetItems(ctx, lo.Uniq(consumedIds), data.GetOptions{})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, item := range consumedRecords {
		consumedGroupKeys[engine.ResolveGroupKey(item)] = struct{}{}
	}
	constrained, report := engine.ApplyWithConsumedGroups(result, items, consumed, consumedGroupKeys, limit)
	r.LastConstraintReport = report
	return constrained, nil
}

// LoadConstraintItems resolves item attributes for constraint candidates in a
// single batch call. It only touches candidates already in hand; there is no
// full-library scan.
func (r *Recommender) LoadConstraintItems(ctx context.Context, scores []cache.Score) (map[string]data.Item, error) {
	ids := make([]string, 0, len(scores))
	for _, score := range scores {
		ids = append(ids, score.Id)
	}
	items, err := r.dataClient.BatchGetItems(ctx, ids, data.GetOptions{})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return lo.SliceToMap(items, func(item data.Item) (string, data.Item) {
		return item.ItemId, item
	}), nil
}

// RecommendSequential recommend items from multiple recommenders sequentially util reaching the limit.
// If limit <= 0, all recommendations are returned.
func (r *Recommender) RecommendSequential(ctx context.Context, result []cache.Score, limit int, names ...string) ([]cache.Score, string, error) {
	var digests []string
	for _, name := range names {
		recommenderFunc, err := r.parse(name)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		scores, digest, err := recommenderFunc(ctx)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		for _, score := range scores {
			r.excludeSet.Add(score.Id)
		}
		result = append(result, scores...)
		digests = append(digests, digest)
		if limit > 0 && len(result) >= limit {
			return result[:limit], util.MD5(digests...), nil
		}
	}
	return result, util.MD5(digests...), nil
}

func (r *Recommender) parse(fullname string) (RecommenderFunc, error) {
	if fullname == CollaborativeRecommender {
		return r.recommendCollaborative, nil
	} else if fullname == LatestRecommender {
		return r.recommendLatest, nil
	} else if after, ok := strings.CutPrefix(fullname, NonPersonalizedRecommender); ok {
		name := after
		return r.recommendNonPersonalized(name), nil
	} else if after, ok := strings.CutPrefix(fullname, ItemToItemRecommender); ok {
		name := after
		return r.recommendItemToItem(name), nil
	} else if after, ok := strings.CutPrefix(fullname, UserToUserRecommender); ok {
		name := after
		return r.recommendUserToUser(name), nil
	} else if after, ok := strings.CutPrefix(fullname, ExternalRecommender); ok {
		name := after
		return r.recommendExternal(name), nil
	} else {
		return nil, errors.Errorf("unknown recommender: %s", fullname)
	}
}

func (r *Recommender) recommendLatest(ctx context.Context) ([]cache.Score, string, error) {
	var after *time.Time
	if r.config.DataSource.ItemTTL > 0 {
		after = new(time.Now().AddDate(0, 0, -int(r.config.DataSource.ItemTTL)))
	}
	items, err := r.dataClient.GetLatestItems(ctx, r.config.CacheSize, r.categories, after)
	if err != nil {
		return nil, "", errors.WithStack(err)
	}
	scores := make([]cache.Score, 0, len(items))
	for _, item := range items {
		if !r.excludeSet.Contains(item.ItemId) {
			scores = append(scores, cache.Score{
				Id:         item.ItemId,
				Score:      float64(item.Timestamp.Unix()),
				Categories: item.Categories,
			})
		}
	}
	return scores, "latest", nil
}

func (r *Recommender) recommendNonPersonalized(name string) RecommenderFunc {
	return func(ctx context.Context) ([]cache.Score, string, error) {
		var categories []string
		if len(r.categories) == 0 {
			categories = []string{""}
		} else {
			categories = r.categories
		}
		// fetch items from cache
		items, err := r.cacheClient.SearchScores(ctx, cache.NonPersonalized, name, categories, 0, r.config.CacheSize)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		// read digest
		digest, err := r.cacheClient.Get(ctx, cache.Key(cache.NonPersonalizedDigest, name)).String()
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		// remove excluded items
		return lo.Filter(items, func(item cache.Score, index int) bool {
			return !r.excludeSet.Contains(item.Id)
		}), digest, nil
	}
}

func (r *Recommender) recommendCollaborative(ctx context.Context) ([]cache.Score, string, error) {
	// fetch items from cache
	items, err := r.cacheClient.SearchScores(ctx, cache.CollaborativeFiltering, r.userId, r.categories, 0, r.config.CacheSize)
	if err != nil {
		return nil, "", errors.WithStack(err)
	}
	// read digest
	digest, err := r.cacheClient.Get(ctx, cache.Key(cache.CollaborativeFilteringDigest, r.userId)).String()
	if err != nil {
		return nil, "", errors.WithStack(err)
	}
	// remove excluded items
	return lo.Filter(items, func(item cache.Score, index int) bool {
		return !r.excludeSet.Contains(item.Id)
	}), digest, nil
}

func (r *Recommender) recommendItemToItem(name string) RecommenderFunc {
	return func(ctx context.Context) ([]cache.Score, string, error) {
		itemToItemConfig := r.config.GetItemToItemConfig(name)
		if itemToItemConfig == nil {
			return nil, "", fmt.Errorf("item-to-item recommender %s %w", name, storage.ErrNotFound)
		}
		data.SortFeedbacks(r.userFeedback)
		userFeedback := make([]data.Feedback, 0, r.config.CacheSize)
		for _, feedback := range r.userFeedback {
			if expression.MatchFeedbackTypeExpressions(r.config.DataSource.PositiveFeedbackTypes, feedback.FeedbackType, feedback.Value) {
				userFeedback = append(userFeedback, feedback)
				if r.online && r.config.ContextSize <= len(userFeedback) {
					break
				}
			}
		}
		scores := make(map[string]float64)
		categories := make(map[string][]string)
		for _, feedback := range userFeedback {
			neighbors, err := QueryItemToItem(ctx, r.vectorClient, *itemToItemConfig, feedback.ItemId, r.categories, r.config.CacheSize)
			if err != nil {
				return nil, "", errors.WithStack(err)
			}
			for _, neighbor := range neighbors {
				if !r.excludeSet.Contains(neighbor.Id) {
					scores[neighbor.Id] += neighbor.Score
					categories[neighbor.Id] = neighbor.Categories
				}
			}
		}
		filter := heap.NewTopKFilter[string, float64](r.config.CacheSize)
		for id, score := range scores {
			filter.Push(id, score)
		}
		elems := filter.PopAll()
		return lo.Map(elems, func(elem heap.Elem[string, float64], _ int) cache.Score {
			return cache.Score{Id: elem.Value, Score: elem.Weight, Categories: categories[elem.Value]}
		}), itemToItemConfig.Hash(&r.config), nil
	}
}

func (r *Recommender) recommendUserToUser(name string) RecommenderFunc {
	return func(ctx context.Context) ([]cache.Score, string, error) {
		userToUserConfig := r.config.GetUserToUserConfig(name)
		if userToUserConfig == nil {
			return nil, "", fmt.Errorf("user-to-user recommender %s %w", name, storage.ErrNotFound)
		}
		scores := make(map[string]float64)
		// load similar users
		similarUsers, err := QueryUserToUser(ctx, r.vectorClient, *userToUserConfig, r.userId, r.config.CacheSize)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		// aggregate scores
		for _, user := range similarUsers {
			// load historical feedback
			feedbacks, err := r.dataClient.GetUserFeedback(ctx, user.Id, new(time.Now()), r.config.DataSource.PositiveFeedbackTypes...)
			if err != nil {
				return nil, "", errors.WithStack(err)
			}
			// add unseen items
			for _, feedback := range feedbacks {
				if !r.excludeSet.Contains(feedback.ItemId) {
					scores[feedback.ItemId] += user.Score
				}
			}
		}
		// collect top k
		filter := heap.NewTopKFilter[string, float64](r.config.CacheSize)
		for id, score := range scores {
			filter.Push(id, score)
		}
		elems := filter.PopAll()
		// filter by categories
		results := make([]cache.Score, 0, len(elems))
		ids := lo.Map(elems, func(elem heap.Elem[string, float64], _ int) string {
			return elem.Value
		})
		var after *time.Time
		if r.config.DataSource.ItemTTL > 0 {
			after = new(time.Now().AddDate(0, 0, -int(r.config.DataSource.ItemTTL)))
		}
		items, err := r.dataClient.BatchGetItems(ctx, ids, data.GetOptions{
			SkipHidden: true,
			After:      after,
		})
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		itemsMap := make(map[string]data.Item)
		for _, item := range items {
			itemsMap[item.ItemId] = item
		}
		for _, elem := range elems {
			if item, ok := itemsMap[elem.Value]; ok && lo.Every(item.Categories, r.categories) {
				results = append(results, cache.Score{
					Id:         item.ItemId,
					Score:      elem.Weight,
					Categories: item.Categories,
				})
			}
		}
		return results, userToUserConfig.Hash(&r.config), nil
	}
}

func (r *Recommender) recommendExternal(name string) RecommenderFunc {
	return func(ctx context.Context) ([]cache.Score, string, error) {
		var externalConfig config.ExternalConfig
		for _, extConfig := range r.config.External {
			if extConfig.Name == name {
				externalConfig = extConfig
				break
			}
		}

		if len(r.categories) > 0 {
			// external recommenders do not support categories
			return nil, externalConfig.Hash(), nil
		}

		external, err := NewExternal(externalConfig)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		defer external.Close()
		items, err := external.Pull(r.userId)
		if err != nil {
			return nil, "", errors.WithStack(err)
		}
		scores := make([]cache.Score, 0, len(items))
		for _, itemId := range items {
			if !r.excludeSet.Contains(itemId) {
				scores = append(scores, cache.Score{
					Id: itemId,
				})
			}
		}
		return scores, externalConfig.Hash(), nil
	}
}

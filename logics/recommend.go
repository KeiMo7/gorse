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
	"github.com/gorse-io/gorse/common/util"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/storage"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/gorse-io/gorse/storage/vectors"
	"github.com/pkg/errors"
	"github.com/samber/lo"
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

	// explain, when enabled, records the provenance of every returned item
	// without changing the recommendation content, order or count.
	explain bool
	// provenance maps an item id to the recommender that produced it.
	provenance map[string]string
	// chainUsage records, in order, every recommender invoked through
	// RecommendSequential together with the number of items it contributed.
	chainUsage []ChainContribution
	// finalScores holds the post-filter scores for the explain builder.
	finalScores []cache.Score
	// offlineFiltered counts offline candidates removed by request-time
	// feedback filtering during RecommendFromOffline.
	offlineFiltered int
}

// ChainContribution describes how many items one recommender contributed.
type ChainContribution struct {
	Source string `json:"source"`
	Count  int    `json:"count"`
}

// Source labels used in the explain output. They are part of the observable
// contract and never contain feedback text or raw item labels.
const (
	SourceOffline       = "offline"
	SourceFallback      = "fallback"
	SourceOnlineLatest  = "online/latest"
	SourceOnlineSession = "online/session"
)

type RecommenderFunc func(ctx context.Context) ([]cache.Score, string, error)

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
		provenance:   make(map[string]string),
	}, nil
}

// EnableExplain turns on provenance tracking. It must be called before
// Recommend; tracking is opt-in so the default path allocates nothing extra.
func (r *Recommender) EnableExplain() *Recommender {
	r.explain = true
	if r.provenance == nil {
		r.provenance = make(map[string]string)
	}
	return r
}

// ExplainEnabled reports whether provenance tracking is active.
func (r *Recommender) ExplainEnabled() bool { return r.explain }

// Provenance returns the source label recorded for an item, or "".
func (r *Recommender) Provenance(itemId string) string { return r.provenance[itemId] }

// ChainUsage returns the per-recommender contributions gathered so far.
func (r *Recommender) ChainUsage() []ChainContribution {
	return append([]ChainContribution(nil), r.chainUsage...)
}

// LatestFeedbackTime returns the most recent feedback timestamp observed when
// the recommender was built, i.e. the online boundary. Zero means no feedback.
func (r *Recommender) LatestFeedbackTime() time.Time {
	var latest time.Time
	for _, feedback := range r.userFeedback {
		if feedback.Timestamp.After(latest) {
			latest = feedback.Timestamp
		}
	}
	return latest
}

// rememberFinal stores the final post-filter scores so the explain builder can
// map only returned items to their provenance.
func (r *Recommender) rememberFinal(scores []cache.Score) {
	if !r.explain {
		return
	}
	r.finalScores = append(r.finalScores[:0], scores...)
}

// RememberFinal is the exported entry used by online handlers to record the
// final, offset-adjusted scores before building the explain response.
func (r *Recommender) RememberFinal(scores []cache.Score) {
	r.rememberFinal(scores)
}

// ExplainedScores returns the final scores captured for the explain output.
func (r *Recommender) ExplainedScores() []cache.Score {
	return append([]cache.Score(nil), r.finalScores...)
}

// recordProvenance tags each item with its producing recommender. In explain
// mode only; otherwise it is a cheap no-op.
func (r *Recommender) recordProvenance(source string, items []cache.Score) {
	if !r.explain {
		return
	}
	contributed := 0
	for _, item := range items {
		if _, exists := r.provenance[item.Id]; !exists {
			r.provenance[item.Id] = source
			contributed++
		}
	}
	if contributed > 0 {
		r.chainUsage = append(r.chainUsage, ChainContribution{Source: source, Count: contributed})
	}
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
		r.recordProvenance(SourceOffline, result)
	} else {
		result, _, err = r.RecommendSequential(ctx, result, r.config.CacheSize, r.config.Ranker.Recommenders...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if len(result) >= limit && limit > 0 {
		return result[:limit], nil
	}
	result, _, err = r.RecommendSequential(ctx, result, limit, r.config.Fallback.Recommenders...)
	return result, errors.WithStack(err)
}

// RecommendFromOffline behaves like Recommend but treats the supplied
// pre-loaded offline scores (read through the versioning resolver) as the
// offline source instead of re-reading the legacy collection. It is used by
// the version-aware online path; content, order, count and fallback semantics
// are identical to Recommend.
func (r *Recommender) RecommendFromOffline(ctx context.Context, offline []cache.Score, limit int) (result []cache.Score, err error) {
	// Count request-time filtered offline candidates before the offline set
	// itself is added to the exclude set.
	r.offlineFiltered = countExcluded(r.excludeSet, offline)
	result = make([]cache.Score, 0, len(offline))
	for _, score := range offline {
		if !r.excludeSet.Contains(score.Id) {
			r.excludeSet.Add(score.Id)
			result = append(result, score)
		}
	}
	r.recordProvenance(SourceOffline, result)
	if limit > 0 && len(result) >= limit {
		return result[:limit], nil
	}
	if limit <= 0 {
		// No bound: still allow fallback to enrich, matching Recommend(0).
		limit = r.config.CacheSize
	}
	result, _, err = r.RecommendSequential(ctx, result, limit, r.config.Fallback.Recommenders...)
	return result, errors.WithStack(err)
}

// OfflineFiltered returns the number of offline candidates removed by
// request-time feedback filtering during the last RecommendFromOffline call.
func (r *Recommender) OfflineFiltered() int { return r.offlineFiltered }

// countExcluded counts offline candidates already present in the request-time
// exclude set (i.e. items interacted with after the offline sample cutoff).
func countExcluded(excludeSet mapset.Set[string], offline []cache.Score) int {
	filtered := 0
	for _, score := range offline {
		if excludeSet.Contains(score.Id) {
			filtered++
		}
	}
	return filtered
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
		// Tag every newly contributed item with its producing recommender.
		// The fallback chain is labeled distinctly so the response can show
		// which chain filled a short offline result set.
		r.recordProvenance(r.chainSourceLabel(name), scores)
		result = append(result, scores...)
		digests = append(digests, digest)
		if limit > 0 && len(result) >= limit {
			return result[:limit], util.MD5(digests...), nil
		}
	}
	return result, util.MD5(digests...), nil
}

// chainSourceLabel maps a recommender name to an explain source label. The
// "latest" recommender reads the live data store at request time, so it is
// reported as an online augmentation rather than a cached fallback.
func (r *Recommender) chainSourceLabel(name string) string {
	if name == LatestRecommender {
		return SourceOnlineLatest
	}
	if r.online {
		return SourceFallback + "/" + name
	}
	return name
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

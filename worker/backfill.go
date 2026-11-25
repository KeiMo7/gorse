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

package worker

import (
	"context"
	"strings"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gorse-io/gorse/common/log"
	"github.com/gorse-io/gorse/logics"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/versioning"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

// BackfillGenerator recomputes and commits one user's recommendation at a
// target version. It shares the pipeline's caches and models so targeted
// backfill produces exactly the same results as a full periodic run, but it
// reuses the target version instead of registering a new one.
type BackfillGenerator struct {
	pipeline *Pipeline
}

// NewBackfillGenerator binds a generator to a pipeline.
func NewBackfillGenerator(p *Pipeline) *BackfillGenerator {
	return &BackfillGenerator{pipeline: p}
}

// Compile-time assertion that BackfillGenerator satisfies the versioning
// Generator contract.
var _ versioning.Generator = (*BackfillGenerator)(nil)

// GenerateForUser recomputes one user and commits at the requested target.
func (g *BackfillGenerator) GenerateForUser(ctx context.Context, userId string, target versioning.Version) error {
	p := g.pipeline
	itemCache := NewItemCache(p.DataClient)
	recommendTime := time.Now().UTC()

	user, err := p.DataClient.GetUser(ctx, userId)
	if err != nil {
		return errors.Wrapf(err, "stage=load_user user=%s", userId)
	}
	recommender, err := logics.NewRecommender(p.Config.Recommend, p.CacheClient, p.DataClient, p.VectorClient, false, userId, nil)
	if err != nil {
		return errors.Wrapf(err, "stage=build_recommender user=%s", userId)
	}

	// Collaborative filtering sub-results, mirroring the periodic pipeline.
	if matrixFactorizationUsers, matrixFactorizationID := p.GetMatrixFactorization(); !strings.EqualFold(p.Config.Recommend.Collaborative.Type, "none") && matrixFactorizationUsers != nil {
		if userEmbedding, ok := matrixFactorizationUsers.Get(userId); ok {
			if err := p.updateCollaborativeRecommend(ctx, matrixFactorizationID, userId, userEmbedding, recommender.ExcludeSet()); err != nil {
				return errors.Wrapf(err, "stage=collaborative user=%s", userId)
			}
		}
	}

	// Candidate recall.
	var (
		scores           []cache.Score
		digest           string
		recommenderNames []string
	)
	if len(p.Config.Recommend.Ranker.Recommenders) > 0 {
		recommenderNames = p.Config.Recommend.Ranker.Recommenders
	} else {
		recommenderNames = p.Config.Recommend.ListRecommenders()
	}
	scores, digest, err = recommender.RecommendSequential(ctx, scores, 0, recommenderNames...)
	if err != nil {
		return errors.Wrapf(err, "stage=recall user=%s", userId)
	}

	candidates := make([]cache.Score, 0, len(scores))
	candidateSet := mapset.NewSet[string]()
	items, err := itemCache.GetMap(ctx, lo.Map(scores, func(score cache.Score, _ int) string {
		return score.Id
	}))
	if err != nil {
		return errors.Wrapf(err, "stage=load_items user=%s", userId)
	}
	for _, score := range scores {
		if _, exist := items[score.Id]; exist {
			score.Timestamp = recommendTime
			candidates = append(candidates, score)
			candidateSet.Add(score.Id)
		}
	}

	var replacementPositiveItems, replacementNegativeItems mapset.Set[string]
	if p.Config.Recommend.Replacement.EnableReplacement && p.Config.Recommend.Ranker.Type != "none" {
		candidates, replacementPositiveItems, replacementNegativeItems, err = p.addReplacementCandidates(
			ctx, candidates, candidateSet, recommender.UserFeedback(), itemCache, recommendTime,
		)
		if err != nil {
			return errors.Wrapf(err, "stage=replacement user=%s", userId)
		}
	}

	// Ranking. API rankers use the request context here rather than the
	// detached worker pool context used by the periodic loop.
	var results []cache.Score
	switch {
	case p.Config.Recommend.Ranker.Type == "fm" && p.ClickThroughRateModel != nil && !p.ClickThroughRateModel.Invalid():
		results, err = p.rankByClickTroughRate(ctx, p.ClickThroughRateModel, &user, candidates, itemCache, recommendTime)
		if err != nil {
			return errors.Wrapf(err, "stage=rank_fm user=%s", userId)
		}
	case p.Config.Recommend.Ranker.Type == "reranker":
		ranker, rErr := logics.NewReranker(p.Config.Recommend.Ranker.RerankerAPI)
		if rErr != nil {
			return errors.Wrapf(rErr, "stage=rank_reranker user=%s", userId)
		}
		results, err = p.rankByReranker(ctx, nil, ranker, &user, recommender.UserFeedback(), candidates, itemCache, recommendTime)
		if err != nil {
			return errors.Wrapf(err, "stage=rank_reranker user=%s", userId)
		}
	case p.Config.Recommend.Ranker.Type == "decision":
		ranker, dErr := logics.NewDecision(p.Config.Recommend.Ranker.DecisionAPI)
		if dErr != nil {
			return errors.Wrapf(dErr, "stage=rank_decision user=%s", userId)
		}
		results, err = p.rankByDecision(ctx, nil, ranker, &user, recommender.UserFeedback(), candidates, itemCache, recommendTime)
		if err != nil {
			return errors.Wrapf(err, "stage=rank_decision user=%s", userId)
		}
	default:
		results = candidates
	}

	if p.Config.Recommend.Replacement.EnableReplacement && p.Config.Recommend.Ranker.Type != "none" {
		results = p.applyReplacementDecay(results, replacementPositiveItems, replacementNegativeItems)
	}

	// Commit at the explicit target version: versioned rows plus legacy mirror.
	if err := p.VersionStore().CommitResult(ctx, userId, target, results, true, digest); err != nil {
		return errors.Wrapf(err, "stage=commit user=%s", userId)
	}
	log.Logger().Info("backfilled user recommendation",
		zap.String("user_id", userId), zap.Int64("version", target.Sequence))
	return nil
}

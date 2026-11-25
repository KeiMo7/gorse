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
	"time"

	"github.com/gorse-io/gorse/storage/cache"
)

// EffectiveVersion identifies the version an online read should serve and
// whether it is a rollback target.
type EffectiveVersion struct {
	Sequence    int64  `json:"sequence"`
	Fingerprint string `json:"fingerprint"`
	Rollback    bool   `json:"rollback"`
}

// ResolveResult bundles the versioned read outcome. Scores may be empty: the
// online recommender then runs the fallback chain, which the response marks
// as a fallback.
type ResolveResult struct {
	// Scores are the versioned offline results, in stored ranking order.
	Scores []cache.Score
	// Pointer is the user's version pointer, nil when absent or corrupt.
	Pointer *UserPointer
	// Effective is the version that was read.
	Effective EffectiveVersion
	// Freshness is the shared verdict computed by the Judge.
	Freshness Freshness
}

// Resolver reads versioned offline results. Online serving uses Resolve so
// that read decisions reuse the exact same metadata and rules as the offline
// write path.
type Resolver struct {
	store *Store
	judge *Judge
}

// NewResolver builds a Resolver over the shared Store.
func NewResolver(store *Store, ageLimit time.Duration) *Resolver {
	return &Resolver{store: store, judge: NewJudge(store, ageLimit)}
}

// Judge exposes the shared freshness judge, used by callers that only need a
// verdict without fetching scores.
func (r *Resolver) Judge() *Judge { return r.judge }

// Store exposes the backing store.
func (r *Resolver) Store() *Store { return r.store }

// Resolve decides the effective version for a user and reads its rows.
//
// The effective sequence is:
//  1. a per-user rollback override, if present;
//  2. the global rollback target, if active and in scope;
//  3. the current version otherwise.
//
// Corrupt or missing metadata never raises: the returned Freshness carries
// the conservative stale/missing state and the caller falls back gracefully.
func (r *Resolver) Resolve(ctx context.Context, userId string, categories []string, begin, end int) (*ResolveResult, error) {
	current, err := r.store.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	globalRollback, err := r.store.Rollback(ctx)
	if err != nil {
		return nil, err
	}
	userRollback, err := r.store.UserRollback(ctx, userId)
	if err != nil {
		return nil, err
	}
	pointer, pointerCorrupt := r.readPointerSafe(ctx, userId)

	// Determine the effective version.
	var (
		effectiveSeq int64
		effectiveFP  string
		isRollback   bool
		rollback     *RollbackState
	)
	switch {
	case userRollback != nil:
		effectiveSeq, isRollback, rollback = userRollback.Sequence, true, userRollback
	case globalRollback != nil && globalRollback.InScope(userId):
		effectiveSeq, isRollback, rollback = globalRollback.Sequence, true, globalRollback
	case current != nil:
		effectiveSeq, effectiveFP = current.Sequence, current.Fingerprint
	}
	if v, err := r.store.VersionBySequence(ctx, effectiveSeq); err == nil && v != nil {
		effectiveFP = v.Fingerprint
	}

	result := &ResolveResult{
		Pointer: pointer,
		Effective: EffectiveVersion{
			Sequence:    effectiveSeq,
			Fingerprint: effectiveFP,
			Rollback:    isRollback,
		},
	}

	if pointerCorrupt {
		result.Freshness = Freshness{
			State:            StateStale,
			Reason:           "version metadata is missing or corrupt, treated as stale",
			ExpectedSequence: effectiveSeq,
			AgeLimit:         r.judge.ageLimit,
			Rollback:         isRollback,
		}
		return result, nil
	}

	result.Freshness = r.judge.Evaluate(ctx, userId, pointer, rollback, current)

	// Read the effective subset when a concrete version is selected.
	if effectiveSeq > 0 {
		scores, err := r.store.cache.SearchScores(
			ctx, RecommendVersioned, Subset(userId, effectiveSeq), categories, begin, end)
		if err != nil {
			return nil, err
		}
		result.Scores = scores
		// An active rollback whose target subset was cleaned up is surfaced as
		// a conservative stale verdict rather than a fabricated rollback hit.
		if isRollback && len(scores) == 0 {
			result.Freshness.State = StateStale
			result.Freshness.Rollback = true
			result.Freshness.Reason = "rollback target results are missing for this user"
		}
	}
	return result, nil
}

// readPointerSafe returns (nil, true) on a corrupt pointer rather than
// failing, matching the conservative degradation contract.
func (r *Resolver) readPointerSafe(ctx context.Context, userId string) (*UserPointer, bool) {
	pointer, err := r.store.ReadUserPointer(ctx, userId)
	if err != nil {
		return nil, true
	}
	return pointer, false
}

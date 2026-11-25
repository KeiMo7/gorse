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
	"github.com/gorse-io/gorse/storage/data"
)

// SelectionScope enumerates the three supported backfill target scopes.
type SelectionScope string

const (
	// ScopeUsers selects an explicit list of user ids.
	ScopeUsers SelectionScope = "users"
	// ScopeModifiedWindow selects users modified within a time window.
	ScopeModifiedWindow SelectionScope = "modified_window"
	// ScopeBehindVersion selects users lagging behind a version.
	ScopeBehindVersion SelectionScope = "behind_version"
)

// Task statuses. They are persisted so an interrupted run can be resumed and
// observed after a process restart.
const (
	TaskPending   = "pending"
	TaskRunning   = "running"
	TaskCanceling = "canceling"
	TaskCompleted = "completed"
	TaskFailed    = "failed"
	TaskCanceled  = "canceled"
)

// Selector describes which users a backfill must cover. Exactly one scope is
// populated according to Scope.
type Selector struct {
	Scope SelectionScope `json:"scope"`
	// UserIDs is used with ScopeUsers.
	UserIDs []string `json:"user_ids,omitempty"`
	// ModifiedAfter / ModifiedBefore bound ScopeModifiedWindow.
	ModifiedAfter  *time.Time `json:"modified_after,omitempty"`
	ModifiedBefore *time.Time `json:"modified_before,omitempty"`
	// BehindSequence selects users whose pointer sequence is below this.
	BehindSequence int64 `json:"behind_sequence,omitempty"`
}

// UserFailure records a single per-user failure with its stage and reason.
type UserFailure struct {
	UserID string `json:"user_id"`
	Stage  string `json:"stage"`
	Reason string `json:"reason"`
}

// Task is the persisted state of one backfill job.
type Task struct {
	ID          string        `json:"id"`
	Selector    Selector      `json:"selector"`
	Target      Version       `json:"target"`
	Status      string        `json:"status"`
	DryRun      bool          `json:"dry_run"`
	Concurrency int           `json:"concurrency"`
	Total       int           `json:"total"`
	Succeeded   int           `json:"succeeded"`
	Failed      int           `json:"failed"`
	Skipped     int           `json:"skipped"`
	Estimated   int           `json:"estimated_items"`
	Failures    []UserFailure `json:"failures,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
	FinishedAt  *time.Time    `json:"finished_at,omitempty"`
	// DoneUsers is the durable resume marker: users already processed are not
	// retried when the task is resumed.
	DoneUsers []string `json:"done_users,omitempty"`
	// VersionDistribution reports candidate counts grouped by their current
	// version sequence (0 = no version).
	VersionDistribution map[int64]int `json:"version_distribution,omitempty"`
}

// DryRunReport is the non-mutating output of a dry run.
type DryRunReport struct {
	Candidates          int           `json:"candidates"`
	VersionDistribution map[int64]int `json:"version_distribution"`
	EstimatedItems      int           `json:"estimated_items"`
	ScopedUsers         []string      `json:"scoped_users"`
}

// Generator produces and commits one user's results for a target version. It
// is implemented by the worker pipeline; the manager depends only on this
// interface so the versioning package stays free of worker/model imports.
type Generator interface {
	// GenerateForUser recomputes and commits results for one user at target.
	GenerateForUser(ctx context.Context, userId string, target Version) error
}

// BackfillStore is the storage surface the manager needs beyond *Store.
type BackfillStore interface {
	// AllUsers lists every user id.
	AllUsers(ctx context.Context) ([]string, error)
	// UserModifiedAt returns the last-modification marker for a user.
	UserModifiedAt(ctx context.Context, userId string) (time.Time, error)
	// EstimatedItems returns the cached result size used for dry-run estimates.
	EstimatedItems(ctx context.Context, userId string) (int, error)
}

// cacheBackfillStore adapts the existing cache/data interfaces.
type cacheBackfillStore struct {
	cache cache.Database
	data  data.Database
}

// NewCacheBackfillStore builds a BackfillStore over the production interfaces.
func NewCacheBackfillStore(cacheClient cache.Database, dataClient data.Database) BackfillStore {
	return &cacheBackfillStore{cache: cacheClient, data: dataClient}
}

func (b *cacheBackfillStore) AllUsers(ctx context.Context) ([]string, error) {
	var users []string
	cursor := ""
	for {
		next, batch, err := b.data.GetUsers(ctx, cursor, 1000)
		if err != nil {
			return nil, err
		}
		for _, u := range batch {
			users = append(users, u.UserId)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	return users, nil
}

func (b *cacheBackfillStore) UserModifiedAt(ctx context.Context, userId string) (time.Time, error) {
	ret := b.cache.Get(ctx, cache.Key(cache.LastModifyUserTime, userId))
	if !ret.Exists() {
		return time.Time{}, nil
	}
	return ret.Time()
}

func (b *cacheBackfillStore) EstimatedItems(ctx context.Context, userId string) (int, error) {
	scores, err := b.cache.SearchScores(ctx, cache.Recommend, userId, nil, 0, -1)
	if err != nil {
		return 0, err
	}
	return len(scores), nil
}

// taskKey returns the cache key for a task record.
func taskKey(id string) string { return cache.Key("versioning_backfill_task", id) }

// taskIndexKey lists task ids.
const taskIndexKey = "versioning_backfill_index"

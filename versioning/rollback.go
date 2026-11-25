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

	"github.com/pkg/errors"
)

// RollbackRequest activates a rollback to a target version for a scope. An
// empty UserIDs scope means every user.
type RollbackRequest struct {
	Sequence int64    `json:"sequence"`
	UserIDs  []string `json:"user_ids,omitempty"`
	Reason   string   `json:"reason"`
}

// MissingUser identifies a user that lacks the target version's results.
type MissingUser struct {
	UserID   string `json:"user_id"`
	Sequence int64  `json:"sequence"`
}

// ActivateRollback validates that every in-scope user retains the target
// version's results before changing any state. If any target is missing the
// switch is rejected and the stored state remains byte-for-byte unchanged.
// During the rollback window no recomputation is performed for in-scope users
// (see Pipeline.checkVersionOutOfDate).
func (m *Manager) ActivateRollback(ctx context.Context, req RollbackRequest) (*RollbackState, []MissingUser, error) {
	target, err := m.store.VersionBySequence(ctx, req.Sequence)
	if err != nil {
		return nil, nil, err
	}
	if target == nil {
		return nil, nil, errors.Wrapf(ErrRollbackTargetMissing, "version %d is not registered", req.Sequence)
	}

	scope := req.UserIDs
	if len(scope) == 0 {
		scope, err = m.backing.AllUsers(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	// Validate all targets before mutating anything.
	var missing []MissingUser
	for _, userId := range scope {
		ok, err := m.store.HasVersionedResult(ctx, userId, req.Sequence)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			missing = append(missing, MissingUser{UserID: userId, Sequence: req.Sequence})
		}
	}
	if len(missing) > 0 {
		// Explicit rejection; no rollback key is written, state is unchanged.
		return nil, missing, errors.Wrapf(ErrRollbackTargetMissing,
			"%d user(s) lack version %d results", len(missing), req.Sequence)
	}

	state := RollbackState{
		Sequence:  req.Sequence,
		Scope:     append([]string(nil), req.UserIDs...),
		Reason:    req.Reason,
		StartedAt: m.now().UTC(),
	}
	if err := m.store.SetRollback(ctx, state); err != nil {
		return nil, nil, err
	}
	return &state, nil, nil
}

// DeactivateRollback switches online reads back to the current version. With
// an empty userIDs it clears the global rollback state and every per-user
// override; otherwise it clears only the named per-user overrides.
func (m *Manager) DeactivateRollback(ctx context.Context, userIDs []string) error {
	if len(userIDs) == 0 {
		if err := m.store.ClearRollback(ctx); err != nil {
			return err
		}
		all, err := m.backing.AllUsers(ctx)
		if err != nil {
			return err
		}
		userIDs = all
	}
	for _, userId := range userIDs {
		if err := m.store.ClearUserRollback(ctx, userId); err != nil {
			return err
		}
	}
	return nil
}

// ActivateUserRollback sets a per-user rollback override after validating the
// target subset exists for that user.
func (m *Manager) ActivateUserRollback(ctx context.Context, userId string, req RollbackRequest) error {
	target, err := m.store.VersionBySequence(ctx, req.Sequence)
	if err != nil {
		return err
	}
	if target == nil {
		return errors.Wrapf(ErrRollbackTargetMissing, "version %d is not registered", req.Sequence)
	}
	ok, err := m.store.HasVersionedResult(ctx, userId, req.Sequence)
	if err != nil {
		return err
	}
	if !ok {
		return errors.Wrapf(ErrRollbackTargetMissing, "user %s lacks version %d results", userId, req.Sequence)
	}
	return m.store.SetUserRollback(ctx, userId, RollbackState{
		Sequence:  req.Sequence,
		Scope:     []string{userId},
		Reason:    req.Reason,
		StartedAt: time.Now().UTC(),
	})
}

// ObservabilitySnapshot is the version-grouped freshness aggregate.
type ObservabilitySnapshot struct {
	// ByVersion counts users grouped by their pointer version sequence.
	ByVersion map[int64]VersionCounters `json:"by_version"`
	// Totals sums every counter across versions.
	Totals VersionCounters `json:"totals"`
	// OldestDataAge / NewestDataAge bound the result age across users.
	OldestDataAge DurationMS `json:"oldest_data_age_ms"`
	NewestDataAge DurationMS `json:"newest_data_age_ms"`
	// CurrentSequence is the registered current version.
	CurrentSequence int64 `json:"current_sequence"`
	// RollbackActive reports whether a rollback is in effect.
	RollbackActive bool `json:"rollback_active"`
	// LatestTasks summarizes recent backfill task state.
	LatestTasks []TaskSummary `json:"latest_tasks"`
}

// VersionCounters holds the four freshness counters for one version.
type VersionCounters struct {
	Fresh    int `json:"fresh"`
	Stale    int `json:"stale"`
	Missing  int `json:"missing"`
	Rollback int `json:"rollback"`
	Total    int `json:"total"`
}

// TaskSummary is the observable, secret-free view of a backfill task.
type TaskSummary struct {
	ID        string `json:"id"`
	Status    string `json:"status"`
	Total     int    `json:"total"`
	Succeeded int    `json:"succeeded"`
	Failed    int    `json:"failed"`
	DryRun    bool   `json:"dry_run"`
}

// DurationMS serializes a duration as milliseconds for stable JSON.
type DurationMS int64

func (d DurationMS) Duration() time.Duration { return time.Duration(d) * time.Millisecond }

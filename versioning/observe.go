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
)

// Observe builds the version-grouped freshness snapshot. It iterates users
// once, reusing the shared Judge, so counters always match what online reads
// would report. It never reads feedback text or item labels.
func (m *Manager) Observe(ctx context.Context, ageLimit time.Duration, maxTasks int) (*ObservabilitySnapshot, error) {
	current, err := m.store.CurrentVersion(ctx)
	if err != nil {
		return nil, err
	}
	globalRollback, err := m.store.Rollback(ctx)
	if err != nil {
		return nil, err
	}
	judge := NewJudge(m.store, ageLimit)
	users, err := m.backing.AllUsers(ctx)
	if err != nil {
		return nil, err
	}

	snapshot := &ObservabilitySnapshot{
		ByVersion: make(map[int64]VersionCounters),
	}
	if current != nil {
		snapshot.CurrentSequence = current.Sequence
	}
	snapshot.RollbackActive = globalRollback != nil

	var oldest, newest time.Duration
	for _, userId := range users {
		pointer, _ := m.store.ReadUserPointer(ctx, userId)
		userRollback, _ := m.store.UserRollback(ctx, userId)
		verdict := judge.Evaluate(ctx, userId, pointer, effectiveRollback(globalRollback, userRollback), current)

		groupSeq := int64(0)
		if pointer != nil {
			groupSeq = pointer.Sequence
		}
		counters := snapshot.ByVersion[groupSeq]
		switch verdict.State {
		case StateFresh:
			counters.Fresh++
		case StateStale:
			counters.Stale++
		case StateMissing:
			counters.Missing++
		case StateRollback:
			counters.Rollback++
		}
		counters.Total++
		snapshot.ByVersion[groupSeq] = counters

		// Roll up totals.
		switch verdict.State {
		case StateFresh:
			snapshot.Totals.Fresh++
		case StateStale:
			snapshot.Totals.Stale++
		case StateMissing:
			snapshot.Totals.Missing++
		case StateRollback:
			snapshot.Totals.Rollback++
		}
		snapshot.Totals.Total++

		if pointer != nil && !pointer.UpdatedAt.IsZero() {
			age := m.now().UTC().Sub(pointer.UpdatedAt.UTC())
			if age < 0 {
				age = 0
			}
			if oldest == 0 || age > oldest {
				oldest = age
			}
			if newest == 0 || age < newest {
				newest = age
			}
		}
	}
	snapshot.OldestDataAge = DurationMS(oldest.Milliseconds())
	snapshot.NewestDataAge = DurationMS(newest.Milliseconds())

	tasks, err := m.ListTasks(ctx)
	if err != nil {
		return nil, err
	}
	if maxTasks > 0 && len(tasks) > maxTasks {
		tasks = tasks[len(tasks)-maxTasks:]
	}
	for _, task := range tasks {
		snapshot.LatestTasks = append(snapshot.LatestTasks, TaskSummary{
			ID:        task.ID,
			Status:    task.Status,
			Total:     task.Total,
			Succeeded: task.Succeeded,
			Failed:    task.Failed,
			DryRun:    task.DryRun,
		})
	}
	return snapshot, nil
}

// effectiveRollback returns the per-user override when present, otherwise the
// global rollback (which may be nil).
func effectiveRollback(global, perUser *RollbackState) *RollbackState {
	if perUser != nil {
		return perUser
	}
	return global
}

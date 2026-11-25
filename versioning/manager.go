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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorse-io/gorse/common/parallel"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/pkg/errors"
)

// Manager runs targeted backfills and rollbacks. It is constructed once per
// process and is safe for concurrent use.
type Manager struct {
	store   *Store
	backing BackfillStore
	now     func() time.Time

	mu      sync.Mutex
	running map[string]*runningTask
}

type runningTask struct {
	cancel context.CancelFunc
	scope  Selector
}

// NewManager builds a Manager over a versioning Store and its backing stores.
func NewManager(store *Store, backing BackfillStore) *Manager {
	return &Manager{
		store:   store,
		backing: backing,
		now:     store.now,
		running: make(map[string]*runningTask),
	}
}

// Store returns the underlying versioning store.
func (m *Manager) Store() *Store { return m.store }

// SelectUsers resolves a selector to the concrete user id list and computes
// the version distribution. It performs no writes, which is also the core of
// the dry-run path.
func (m *Manager) SelectUsers(ctx context.Context, sel Selector) ([]string, map[int64]int, error) {
	all, err := m.backing.AllUsers(ctx)
	if err != nil {
		return nil, nil, err
	}
	distribution := make(map[int64]int)
	selected := make([]string, 0)
	for _, userId := range all {
		include, err := m.matches(ctx, sel, userId)
		if err != nil {
			return nil, nil, err
		}
		if !include {
			continue
		}
		selected = append(selected, userId)
		seq := int64(0)
		if pointer, err := m.store.ReadUserPointer(ctx, userId); err == nil && pointer != nil {
			seq = pointer.Sequence
		}
		distribution[seq]++
	}
	return selected, distribution, nil
}

// matches reports whether a user belongs to a selector scope.
func (m *Manager) matches(ctx context.Context, sel Selector, userId string) (bool, error) {
	switch sel.Scope {
	case ScopeUsers:
		for _, id := range sel.UserIDs {
			if id == userId {
				return true, nil
			}
		}
		return false, nil
	case ScopeModifiedWindow:
		modifiedAt, err := m.backing.UserModifiedAt(ctx, userId)
		if err != nil {
			return false, err
		}
		if sel.ModifiedAfter != nil && !modifiedAt.After(*sel.ModifiedAfter) {
			return false, nil
		}
		if sel.ModifiedBefore != nil && !modifiedAt.Before(*sel.ModifiedBefore) {
			return false, nil
		}
		return true, nil
	case ScopeBehindVersion:
		pointer, err := m.store.ReadUserPointer(ctx, userId)
		if err != nil {
			// Corrupt pointer: treat as behind so it gets repaired.
			return true, nil
		}
		if pointer == nil {
			return true, nil
		}
		return pointer.Sequence < sel.BehindSequence, nil
	default:
		return false, errors.Errorf("unknown backfill scope: %s", sel.Scope)
	}
}

// DryRun resolves the selector without writing anything, returning candidate
// counts, version distribution and an estimated result volume.
func (m *Manager) DryRun(ctx context.Context, sel Selector) (*DryRunReport, error) {
	users, distribution, err := m.SelectUsers(ctx, sel)
	if err != nil {
		return nil, err
	}
	estimated := 0
	for _, userId := range users {
		n, err := m.backing.EstimatedItems(ctx, userId)
		if err != nil {
			return nil, err
		}
		estimated += n
	}
	return &DryRunReport{
		Candidates:          len(users),
		VersionDistribution: distribution,
		EstimatedItems:      estimated,
		ScopedUsers:         users,
	}, nil
}

// StartBackfill creates and starts a backfill task. A concurrent task with an
// overlapping scope is either merged (identical scope) or explicitly rejected.
// Dry runs never execute the generator.
func (m *Manager) StartBackfill(
	ctx context.Context,
	sel Selector,
	target Version,
	dryRun bool,
	concurrency int,
	maxFailures int,
	gen Generator,
) (*Task, error) {
	if concurrency <= 0 {
		concurrency = 1
	}
	// Resolve candidates up front so the task carries a stable total.
	users, distribution, err := m.SelectUsers(ctx, sel)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	// Conflict handling against in-process running tasks.
	for id, rt := range m.running {
		if scopesEqual(rt.scope, sel) {
			// Merge: hand back the existing task id instead of double-writing.
			existing, gErr := m.GetTask(ctx, id)
			m.mu.Unlock()
			if gErr != nil {
				return nil, gErr
			}
			return existing, nil
		}
		if scopesOverlap(rt.scope, sel, users) {
			m.mu.Unlock()
			return nil, errors.Wrapf(ErrConflictingBackfill, "task %s covers overlapping users", id)
		}
	}

	now := m.now().UTC()
	task := &Task{
		ID:                  fmt.Sprintf("bf-%d", now.UnixNano()),
		Selector:            sel,
		Target:              target,
		Status:              TaskPending,
		DryRun:              dryRun,
		Concurrency:         concurrency,
		Total:               len(users),
		VersionDistribution: distribution,
		CreatedAt:           now,
		UpdatedAt:           now,
		DoneUsers:           []string{},
	}
	if maxFailures > 0 {
		task.Failures = make([]UserFailure, 0, min(maxFailures, 64))
	}
	taskCtx, cancel := context.WithCancel(context.Background())
	m.running[task.ID] = &runningTask{cancel: cancel, scope: sel}
	m.mu.Unlock()

	if err := m.persistTask(ctx, task); err != nil {
		m.removeRunning(task.ID)
		return nil, err
	}

	if dryRun {
		report, err := m.DryRun(ctx, sel)
		if err != nil {
			return nil, err
		}
		task.Estimated = report.EstimatedItems
		task.Status = TaskCompleted
		finished := m.now().UTC()
		task.FinishedAt = &finished
		if err := m.persistTask(ctx, task); err != nil {
			return nil, err
		}
		m.removeRunning(task.ID)
		return task, nil
	}

	go m.run(taskCtx, task, users, gen, maxFailures)
	return task, nil
}

// run executes the bounded-concurrency backfill with cancellation and
// per-user failure capture. Already-done users are skipped for resume.
func (m *Manager) run(ctx context.Context, task *Task, users []string, gen Generator, maxFailures int) {
	defer m.removeRunning(task.ID)

	doneSet := make(map[string]struct{}, len(task.DoneUsers))
	for _, id := range task.DoneUsers {
		doneSet[id] = struct{}{}
	}
	pending := make([]string, 0, len(users))
	for _, id := range users {
		if _, ok := doneSet[id]; !ok {
			pending = append(pending, id)
		}
	}
	task.Status = TaskRunning
	_ = m.persistTask(ctx, task)

	var (
		mu      sync.Mutex
		failBuf []UserFailure
	)
	// Bounded concurrency via the project's standard worker pool.
	err := parallel.ForEach(ctx, pending, task.Concurrency, func(_ int, userId string) {
		if ctx.Err() != nil {
			return
		}
		stage := "generate"
		gErr := gen.GenerateForUser(ctx, userId, task.Target)
		mu.Lock()
		defer mu.Unlock()
		if gErr != nil {
			task.Failed++
			if maxFailures == 0 || len(failBuf) < maxFailures {
				failBuf = append(failBuf, UserFailure{UserID: userId, Stage: stage, Reason: gErr.Error()})
			}
		} else {
			task.Succeeded++
			task.DoneUsers = append(task.DoneUsers, userId)
		}
		task.UpdatedAt = m.now().UTC()
	})

	mu.Lock()
	task.Failures = append(task.Failures, failBuf...)
	if maxFailures > 0 && len(task.Failures) > maxFailures {
		task.Failures = task.Failures[:maxFailures]
	}
	mu.Unlock()

	now := m.now().UTC()
	task.UpdatedAt = now
	switch {
	case ctx.Err() != nil:
		// Cancellation keeps durable progress so a later run resumes, rather
		// than starting over.
		task.Status = TaskCanceled
		task.FinishedAt = &now
	case err != nil:
		task.Status = TaskFailed
		task.FinishedAt = &now
	default:
		task.Status = TaskCompleted
		task.FinishedAt = &now
	}
	_ = m.persistTask(context.Background(), task)
}

// Cancel marks a running task for cancellation. Its durable progress is kept
// for resume; only future user processing stops.
func (m *Manager) Cancel(ctx context.Context, taskID string) error {
	m.mu.Lock()
	rt, ok := m.running[taskID]
	m.mu.Unlock()
	if !ok {
		// Persisted but not currently executing: record the intent.
		task, err := m.GetTask(ctx, taskID)
		if err != nil {
			return err
		}
		if task.Status == TaskCompleted || task.Status == TaskCanceled {
			return errors.Wrap(ErrTaskNotRunning, taskID)
		}
		task.Status = TaskCanceled
		finished := m.now().UTC()
		task.FinishedAt = &finished
		return m.persistTask(ctx, task)
	}
	rt.cancel()
	task, err := m.GetTask(ctx, taskID)
	if err == nil && task != nil {
		task.Status = TaskCanceling
		_ = m.persistTask(ctx, task)
	}
	return nil
}

// Resume continues an unfinished task, skipping already-done users. It is
// idempotent: resuming a fully successful completed task returns it unchanged.
// A task that finished with per-user failures is retried, but only for the
// users not present in the durable done-set, so it never starts from scratch.
func (m *Manager) Resume(ctx context.Context, taskID string, gen Generator) (*Task, error) {
	task, err := m.GetTask(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, errors.Wrap(ErrTaskNotFound, taskID)
	}
	if task.Status == TaskRunning {
		return task, nil
	}
	if task.Status == TaskCompleted && task.Failed == 0 {
		return task, nil
	}
	// Reset failure bookkeeping for the retry; succeeded/done users are kept.
	task.Failed = 0
	task.Failures = nil
	users, _, err := m.SelectUsers(ctx, task.Selector)
	if err != nil {
		return nil, err
	}
	task.Status = TaskPending
	if err := m.persistTask(ctx, task); err != nil {
		return nil, err
	}
	taskCtx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.running[task.ID] = &runningTask{cancel: cancel, scope: task.Selector}
	m.mu.Unlock()
	go m.run(taskCtx, task, users, gen, cap(task.Failures))
	return task, nil
}

func (m *Manager) removeRunning(id string) {
	m.mu.Lock()
	delete(m.running, id)
	m.mu.Unlock()
}

// GetTask loads a persisted task.
func (m *Manager) GetTask(ctx context.Context, id string) (*Task, error) {
	ret := m.store.cache.Get(ctx, taskKey(id))
	if !ret.Exists() {
		return nil, errors.Wrap(ErrTaskNotFound, id)
	}
	payload, err := ret.String()
	if err != nil {
		return nil, err
	}
	var task Task
	if err := json.Unmarshal([]byte(payload), &task); err != nil {
		return nil, err
	}
	return &task, nil
}

// ListTasks loads all persisted task ids and their records.
func (m *Manager) ListTasks(ctx context.Context) ([]*Task, error) {
	idRet := m.store.cache.Get(ctx, taskIndexKey)
	var ids []string
	if idRet.Exists() {
		if payload, err := idRet.String(); err == nil && payload != "" {
			_ = json.Unmarshal([]byte(payload), &ids)
		}
	}
	tasks := make([]*Task, 0, len(ids))
	for _, id := range ids {
		task, err := m.GetTask(ctx, id)
		if err != nil || task == nil {
			continue
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (m *Manager) persistTask(ctx context.Context, task *Task) error {
	payload, err := json.Marshal(task)
	if err != nil {
		return err
	}
	if err := m.store.cache.Set(ctx, cache.String(taskKey(task.ID), string(payload))); err != nil {
		return err
	}
	// Maintain the id index.
	idRet := m.store.cache.Get(ctx, taskIndexKey)
	var ids []string
	if idRet.Exists() {
		if p, pErr := idRet.String(); pErr == nil && p != "" {
			_ = json.Unmarshal([]byte(p), &ids)
		}
	}
	found := false
	for _, id := range ids {
		if id == task.ID {
			found = true
			break
		}
	}
	if !found {
		ids = append(ids, task.ID)
		indexPayload, _ := json.Marshal(ids)
		return m.store.cache.Set(ctx, cache.String(taskIndexKey, string(indexPayload)))
	}
	return nil
}

// scopesEqual reports whether two selectors target the same set semantics.
func scopesEqual(a, b Selector) bool {
	if a.Scope != b.Scope {
		return false
	}
	switch a.Scope {
	case ScopeUsers:
		return equalStringSet(a.UserIDs, b.UserIDs)
	case ScopeModifiedWindow:
		return timePtrEqual(a.ModifiedAfter, b.ModifiedAfter) && timePtrEqual(a.ModifiedBefore, b.ModifiedBefore)
	case ScopeBehindVersion:
		return a.BehindSequence == b.BehindSequence
	default:
		return false
	}
}

// scopesOverlap is a conservative check: an explicit-user scope that shares a
// user with a running scope is treated as overlapping.
func scopesOverlap(running, incoming Selector, incomingUsers []string) bool {
	if running.Scope != ScopeUsers || incoming.Scope != ScopeUsers {
		// Window/version scopes are broad; reject rather than risk double writes.
		return true
	}
	have := make(map[string]struct{}, len(running.UserIDs))
	for _, id := range running.UserIDs {
		have[id] = struct{}{}
	}
	for _, id := range incoming.UserIDs {
		if _, ok := have[id]; ok {
			return true
		}
	}
	return false
}

func equalStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	have := make(map[string]struct{}, len(a))
	for _, x := range a {
		have[x] = struct{}{}
	}
	for _, x := range b {
		if _, ok := have[x]; !ok {
			return false
		}
	}
	return true
}

func timePtrEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

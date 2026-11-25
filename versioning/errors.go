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

import "errors"

var (
	// errCorruptVersionMeta is returned when version metadata cannot be
	// parsed. Callers treat corrupt metadata as the most conservative stale
	// state instead of failing the request.
	errCorruptVersionMeta = errors.New("corrupt version metadata")
	// ErrRollbackTargetMissing is returned when a rollback is requested to a
	// version whose results have been cleaned up for a target user.
	ErrRollbackTargetMissing = errors.New("rollback target version is missing")
	// ErrNoCurrentVersion is returned when no generation version is active.
	ErrNoCurrentVersion = errors.New("no current recommendation version")
	// ErrConflictingBackfill is returned when a backfill overlaps a running one.
	ErrConflictingBackfill = errors.New("a conflicting backfill task is already running")
	// ErrTaskNotFound is returned when a backfill task id does not exist.
	ErrTaskNotFound = errors.New("backfill task not found")
	// ErrTaskNotRunning is returned when cancelling a finished task.
	ErrTaskNotRunning = errors.New("backfill task is not running")
)

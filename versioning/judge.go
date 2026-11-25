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

// Read state labels exposed by the freshness judge. They are part of the
// observable output contract and must stay stable.
const (
	StateFresh    = "fresh"
	StateStale    = "stale"
	StateMissing  = "missing"
	StateRollback = "rollback"
)

// Freshness is the shared, read-side verdict for one user's offline results.
// The same computation runs inside the offline pipeline (to decide whether a
// user must be recomputed) and inside the online server (to explain reads).
type Freshness struct {
	// State is one of StateFresh, StateStale, StateMissing, StateRollback.
	State string `json:"state"`
	// Reason is a human-readable, secret-free explanation of the verdict.
	Reason string `json:"reason"`
	// ExpectedSequence is the sequence the reader considers current, or the
	// rollback target while a rollback is active.
	ExpectedSequence int64 `json:"expected_sequence"`
	// ActualSequence is the version sequence the user pointer references.
	ActualSequence int64 `json:"actual_sequence"`
	// VersionsBehind is how many registered versions lag behind expected.
	VersionsBehind int64 `json:"versions_behind"`
	// DataAge is the age of the user's results at judgment time.
	DataAge time.Duration `json:"data_age"`
	// AgeLimit is the configured maximum data age.
	AgeLimit time.Duration `json:"age_limit"`
	// UpdatedAt is the result generation timestamp.
	UpdatedAt time.Time `json:"updated_at"`
	// Rollback reports whether the verdict is driven by an active rollback.
	Rollback bool `json:"rollback"`
}

// Judge computes freshness. It holds no per-user state and is safe for
// concurrent use by many readers.
type Judge struct {
	store    *Store
	ageLimit time.Duration
	now      func() time.Time
}

// NewJudge builds a Judge sharing the same Store and clock as the writer.
func NewJudge(store *Store, ageLimit time.Duration) *Judge {
	return &Judge{store: store, ageLimit: ageLimit, now: store.now}
}

// Evaluate computes the freshness verdict. It never returns an error for
// missing or corrupt metadata: those degrade to the conservative states
// (missing / stale) with a readable reason, so an online request never fails
// because version metadata is unavailable.
func (j *Judge) Evaluate(
	ctx context.Context,
	userId string,
	pointer *UserPointer,
	rollback *RollbackState,
	current *Version,
) Freshness {
	now := j.now().UTC()

	// No current generation registered yet: nothing can be fresh.
	if current == nil {
		return Freshness{State: StateMissing, Reason: "no current recommendation version is registered", AgeLimit: j.ageLimit}
	}

	// Rollback takes precedence: the expected version is the rollback target.
	if rollback != nil && rollback.InScope(userId) {
		f := Freshness{
			State:            StateRollback,
			Reason:           "serving rollback target version",
			ExpectedSequence: rollback.Sequence,
			Rollback:         true,
			AgeLimit:         j.ageLimit,
		}
		if pointer == nil {
			// The user has no retained results at all; nothing can be served
			// from the rollback target either.
			f.State = StateMissing
			f.Reason = "rollback active but user has no retained results"
			return f
		}
		f.ActualSequence = pointer.Sequence
		f.UpdatedAt = pointer.UpdatedAt
		f.DataAge = ageOf(pointer.UpdatedAt, now)
		return f
	}

	// No per-user pointer at all: the user has never been generated.
	if pointer == nil {
		return Freshness{
			State:            StateMissing,
			Reason:           "no version metadata for user",
			ExpectedSequence: current.Sequence,
			AgeLimit:         j.ageLimit,
		}
	}

	f := Freshness{
		ExpectedSequence: current.Sequence,
		ActualSequence:   pointer.Sequence,
		UpdatedAt:        pointer.UpdatedAt,
		DataAge:          ageOf(pointer.UpdatedAt, now),
		AgeLimit:         j.ageLimit,
	}

	// Version mismatch is authoritative regardless of age.
	if pointer.Fingerprint != current.Fingerprint || pointer.Sequence != current.Sequence {
		f.State = StateStale
		f.VersionsBehind = j.versionsBehind(ctx, pointer.Sequence, current.Sequence)
		f.Reason = "user results were generated for an older version"
		return f
	}

	// Same version: still stale once the data age limit is exceeded.
	if j.ageLimit > 0 && f.DataAge > j.ageLimit {
		f.State = StateStale
		f.Reason = "results exceed the maximum data age"
		return f
	}

	f.State = StateFresh
	f.Reason = "results match the current version"
	return f
}

// versionsBehind returns the registered-version distance between the user's
// version and the expected one. If the user version is unknown it is reported
// conservatively as at least one version behind.
func (j *Judge) versionsBehind(ctx context.Context, actual, expected int64) int64 {
	if actual <= 0 || expected <= actual {
		if expected > actual && actual > 0 {
			return expected - actual
		}
		return 1
	}
	return expected - actual
}

func ageOf(updatedAt, now time.Time) time.Duration {
	if updatedAt.IsZero() {
		return 0
	}
	d := now.Sub(updatedAt.UTC())
	if d < 0 {
		return 0
	}
	return d
}

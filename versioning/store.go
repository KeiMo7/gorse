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
	"strconv"
	"strings"
	"time"

	"github.com/gorse-io/gorse/storage/cache"
)

const (
	// VersionRegistry stores the ordered list of every registered generation.
	VersionRegistry = "versioning_registry"
	// VersionCurrent points at the generation readers should consider current.
	VersionCurrent = "versioning_current"
	// VersionRollback holds an active rollback state, or is absent otherwise.
	VersionRollback = "versioning_rollback"
	// UserPointerPrefix stores the per-user effective version pointer.
	UserPointerPrefix = "versioning_user_pointer"
	// UserRollbackPrefix stores a per-user rollback override.
	UserRollbackPrefix = "versioning_user_rollback"
	// RecommendVersioned is the score collection holding versioned per-user
	// offline results. The subset encodes the user and the version sequence.
	RecommendVersioned = "recommend_versioned"

	// subsetSeparator separates the user id and the version sequence inside a
	// versioned score subset. User ids cannot contain "/" (util.ValidateId)
	// and the separator "#" is additionally guarded when parsing.
	subsetSeparator = "#"

	defaultHistoryLimit = 3
)

// UserVersionEntry records one version retained for a user.
type UserVersionEntry struct {
	Fingerprint string    `json:"fingerprint"`
	Sequence    int64     `json:"sequence"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// UserPointer is the per-user version metadata. It is written only after the
// versioned result rows are durable, so flipping the pointer is the single
// atomic visibility switch and readers never observe a half version.
type UserPointer struct {
	Fingerprint string             `json:"fingerprint"`
	Sequence    int64              `json:"sequence"`
	UpdatedAt   time.Time          `json:"updated_at"`
	History     []UserVersionEntry `json:"history"`
}

// RollbackState describes an active rollback. An empty Scope means the
// rollback applies to every user; otherwise it applies to the listed users.
type RollbackState struct {
	Sequence  int64     `json:"sequence"`
	Scope     []string  `json:"scope"`
	Reason    string    `json:"reason"`
	StartedAt time.Time `json:"started_at"`
}

// InScope reports whether the rollback applies to userId.
func (r *RollbackState) InScope(userId string) bool {
	if len(r.Scope) == 0 {
		return true
	}
	for _, id := range r.Scope {
		if id == userId {
			return true
		}
	}
	return false
}

// Store persists version metadata on top of the existing cache interface. It
// needs no transaction support: every multi-step write finishes the data
// rows before flipping a single pointer key.
type Store struct {
	cache        cache.Database
	historyLimit int
	now          func() time.Time
}

// NewStore wraps a cache database. historyLimit bounds how many old versions
// are retained per user for rollback; non-positive values use the default.
func NewStore(client cache.Database, historyLimit int) *Store {
	if historyLimit <= 0 {
		historyLimit = defaultHistoryLimit
	}
	return &Store{cache: client, historyLimit: historyLimit, now: time.Now}
}

// WithClock overrides the clock, mainly for deterministic tests.
func (s *Store) WithClock(clock func() time.Time) *Store {
	s.now = clock
	return s
}

// Subset encodes the versioned score subset for a user and version.
func Subset(userId string, sequence int64) string {
	return userId + subsetSeparator + strconv.FormatInt(sequence, 10)
}

// parseSubset decodes a versioned subset. A malformed subset returns ok=false
// rather than a fabricated user id.
func parseSubset(subset string) (userId string, sequence int64, ok bool) {
	idx := strings.LastIndex(subset, subsetSeparator)
	if idx <= 0 || idx == len(subset)-1 {
		return "", 0, false
	}
	seq, err := strconv.ParseInt(subset[idx+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return subset[:idx], seq, true
}

func userPointerKey(userId string) string {
	return cache.Key(UserPointerPrefix, userId)
}

// readString fetches a value key. Missing keys return ("", false, nil);
// storage and parse errors are surfaced to the caller.
func (s *Store) readString(ctx context.Context, key string) (string, bool, error) {
	ret := s.cache.Get(ctx, key)
	if !ret.Exists() {
		return "", false, nil
	}
	value, err := ret.String()
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func userRollbackKey(userId string) string {
	return cache.Key(UserRollbackPrefix, userId)
}

// loadRegistry returns the registered versions in ascending sequence order.
func (s *Store) loadRegistry(ctx context.Context) ([]Version, error) {
	payload, exists, err := s.readString(ctx, VersionRegistry)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	var versions []Version
	if err := unmarshalJSON(payload, &versions); err != nil {
		return nil, err
	}
	return versions, nil
}

func (s *Store) saveRegistry(ctx context.Context, versions []Version) error {
	payload, err := marshalJSON(versions)
	if err != nil {
		return err
	}
	return s.cache.Set(ctx, cache.String(VersionRegistry, payload))
}

// Versions returns all registered generation versions, oldest first.
func (s *Store) Versions(ctx context.Context) ([]Version, error) {
	return s.loadRegistry(ctx)
}

// CurrentVersion returns the active current version.
func (s *Store) CurrentVersion(ctx context.Context) (*Version, error) {
	payload, exists, err := s.readString(ctx, VersionCurrent)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	var v Version
	if err := unmarshalJSON(payload, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// SetCurrentVersion marks v as the current generation. The registry is
// expected to already contain v (see GetOrRegisterVersion).
func (s *Store) SetCurrentVersion(ctx context.Context, v Version) error {
	payload, err := marshalJSON(v)
	if err != nil {
		return err
	}
	return s.cache.Set(ctx, cache.String(VersionCurrent, payload))
}

// GetOrRegisterVersion returns the existing version for the components when
// their fingerprint is already registered (equivalent re-runs are stable),
// otherwise it appends a new version with the next monotonic sequence.
func (s *Store) GetOrRegisterVersion(ctx context.Context, components Components) (Version, bool, error) {
	versions, err := s.loadRegistry(ctx)
	if err != nil {
		return Version{}, false, err
	}
	fingerprint := components.Fingerprint()
	for _, v := range versions {
		if v.Fingerprint == fingerprint {
			return v, false, nil
		}
	}
	var nextSeq int64 = 1
	for _, v := range versions {
		if v.Sequence >= nextSeq {
			nextSeq = v.Sequence + 1
		}
	}
	created := Version{
		Sequence:    nextSeq,
		Fingerprint: fingerprint,
		Components:  components,
		CreatedAt:   s.now().UTC(),
	}
	versions = append(versions, created)
	if err := s.saveRegistry(ctx, versions); err != nil {
		return Version{}, false, err
	}
	return created, true, nil
}

// ReadUserPointer returns the per-user pointer. A missing pointer returns
// (nil, nil); a corrupt pointer returns an error so the caller can degrade to
// the conservative stale state.
func (s *Store) ReadUserPointer(ctx context.Context, userId string) (*UserPointer, error) {
	payload, exists, err := s.readString(ctx, userPointerKey(userId))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	var pointer UserPointer
	if err := unmarshalJSON(payload, &pointer); err != nil {
		return nil, err
	}
	if pointer.Fingerprint == "" || pointer.Sequence <= 0 {
		return nil, errCorruptVersionMeta
	}
	return &pointer, nil
}

func (s *Store) writeUserPointer(ctx context.Context, userId string, pointer UserPointer) error {
	payload, err := marshalJSON(pointer)
	if err != nil {
		return err
	}
	return s.cache.Set(ctx, cache.String(userPointerKey(userId), payload))
}

// CommitResult writes versioned offline results and flips the user pointer.
//
// The sequence guarantees visibility ordering:
//  1. Versioned result rows are upserted (invisible until the pointer points
//     at their subset).
//  2. The pointer, including bounded history, is written as a single key.
//
// When mirrorLegacy is true the same rows are also written to the legacy
// "recommend" collection and the legacy digest/time keys are refreshed, so
// deployments that have not rolled the read path yet keep working.
func (s *Store) CommitResult(
	ctx context.Context,
	userId string,
	version Version,
	results []cache.Score,
	mirrorLegacy bool,
	legacyDigest string,
) error {
	now := s.now().UTC()
	// Stamp every row with the commit time. The legacy cleanup deletes rows
	// older than this instant, so zero-timestamp rows would be removed
	// immediately; normalizing here makes the write self-consistent.
	for i := range results {
		if results[i].Timestamp.IsZero() {
			results[i].Timestamp = now
		}
	}
	// 1. durable versioned rows.
	if err := s.cache.AddScores(ctx, RecommendVersioned, Subset(userId, version.Sequence), results); err != nil {
		return err
	}
	if mirrorLegacy {
		if err := s.cache.AddScores(ctx, cache.Recommend, userId, results); err != nil {
			return err
		}
		if err := s.cache.DeleteScores(ctx, []string{cache.Recommend}, cache.ScoreCondition{
			Before: &now,
			Subset: &userId,
		}); err != nil {
			return err
		}
	}
	// 2. single-key pointer flip.
	pointer, err := s.ReadUserPointer(ctx, userId)
	if err != nil {
		// corrupt or missing pointer: start a fresh one, never fail the write.
		pointer = nil
	}
	entry := UserVersionEntry{
		Fingerprint: version.Fingerprint,
		Sequence:    version.Sequence,
		UpdatedAt:   now,
	}
	if pointer == nil {
		pointer = &UserPointer{
			Fingerprint: version.Fingerprint,
			Sequence:    version.Sequence,
			UpdatedAt:   now,
			History:     []UserVersionEntry{entry},
		}
	} else {
		pointer.Fingerprint = version.Fingerprint
		pointer.Sequence = version.Sequence
		pointer.UpdatedAt = now
		pointer.History = appendVersionHistory(pointer.History, entry, s.historyLimit)
	}
	if err := s.writeUserPointer(ctx, userId, *pointer); err != nil {
		return err
	}
	if mirrorLegacy {
		values := []cache.Value{
			cache.Time(cache.Key(cache.RecommendUpdateTime, userId), now),
		}
		if legacyDigest != "" {
			values = append(values, cache.String(cache.Key(cache.RecommendDigest, userId), legacyDigest))
		}
		if err := s.cache.Set(ctx, values...); err != nil {
			return err
		}
	}
	return nil
}

// appendVersionHistory prepends entry and trims to limit, dropping duplicate
// sequences (a re-run of the same version refreshes its entry instead).
func appendVersionHistory(history []UserVersionEntry, entry UserVersionEntry, limit int) []UserVersionEntry {
	merged := make([]UserVersionEntry, 0, len(history)+1)
	merged = append(merged, entry)
	for _, h := range history {
		if h.Sequence != entry.Sequence {
			merged = append(merged, h)
		}
	}
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

// HasVersionedResult reports whether a user retains results for a version.
func (s *Store) HasVersionedResult(ctx context.Context, userId string, sequence int64) (bool, error) {
	scores, err := s.cache.SearchScores(ctx, RecommendVersioned, Subset(userId, sequence), nil, 0, 1)
	if err != nil {
		return false, err
	}
	return len(scores) > 0, nil
}

// DeleteVersionedResult removes a user's rows for one version, used when a
// version falls outside the retained history.
func (s *Store) DeleteVersionedResult(ctx context.Context, userId string, sequence int64) error {
	return s.cache.DeleteScores(ctx, []string{RecommendVersioned}, cache.ScoreCondition{
		Subset: loString(Subset(userId, sequence)),
	})
}

func loString(v string) *string { return &v }

// SetRollback activates or replaces the global rollback state.
func (s *Store) SetRollback(ctx context.Context, state RollbackState) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.cache.Set(ctx, cache.String(VersionRollback, string(payload)))
}

// ClearRollback removes the global rollback state.
func (s *Store) ClearRollback(ctx context.Context) error {
	return s.cache.Delete(ctx, VersionRollback)
}

// Rollback returns the active global rollback state, or nil when none exists.
// A corrupt payload is treated as absent so a bad write cannot wedge serving.
func (s *Store) Rollback(ctx context.Context) (*RollbackState, error) {
	payload, exists, err := s.readString(ctx, VersionRollback)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	var state RollbackState
	if err := json.Unmarshal([]byte(payload), &state); err != nil {
		return nil, nil
	}
	if state.Sequence <= 0 {
		return nil, nil
	}
	return &state, nil
}

// SetUserRollback activates a per-user rollback override.
func (s *Store) SetUserRollback(ctx context.Context, userId string, state RollbackState) error {
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return s.cache.Set(ctx, cache.String(userRollbackKey(userId), string(payload)))
}

// ClearUserRollback removes a per-user rollback override.
func (s *Store) ClearUserRollback(ctx context.Context, userId string) error {
	return s.cache.Delete(ctx, userRollbackKey(userId))
}

// UserRollback returns the per-user rollback override, or nil.
func (s *Store) UserRollback(ctx context.Context, userId string) (*RollbackState, error) {
	payload, exists, err := s.readString(ctx, userRollbackKey(userId))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	var state RollbackState
	if err := json.Unmarshal([]byte(payload), &state); err != nil {
		return nil, nil
	}
	if state.Sequence <= 0 {
		return nil, nil
	}
	return &state, nil
}

// VersionBySequence looks up a registered version by sequence.
func (s *Store) VersionBySequence(ctx context.Context, sequence int64) (*Version, error) {
	versions, err := s.loadRegistry(ctx)
	if err != nil {
		return nil, err
	}
	for i := range versions {
		if versions[i].Sequence == sequence {
			return &versions[i], nil
		}
	}
	return nil, nil
}

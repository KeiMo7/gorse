// Copyright 2026 gorse Project Authors
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

// Package versioning is the single source of truth for recommendation
// version identity, freshness judgment, targeted backfill and rollback.
//
// Both the offline write path (worker pipeline) and the online read path
// (server REST handlers) use the same Components fingerprint and the same
// Judge, so a version can never be computed twice with different rules.
package versioning

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorse-io/gorse/common/util"
	"github.com/gorse-io/gorse/config"
)

// Components is the comparable identity of a recommendation generation.
// Any change to one of the fields produces a new fingerprint, while an
// equivalent re-run with identical fields produces the same fingerprint.
//
// Components never carries secret fields (API tokens), raw feedback text or
// raw item labels: only digests, identifiers and timestamps are stored.
type Components struct {
	// RecallDigest identifies the recall set: the configured recall
	// recommenders and their parameters, derived from RecommendConfig.Hash.
	RecallDigest string `json:"recall_digest"`
	// RankerType is the ranking method, e.g. "none", "fm", "reranker".
	RankerType string `json:"ranker_type"`
	// RankerDigest identifies ranker parameters, computed without secrets.
	RankerDigest string `json:"ranker_digest"`
	// ModelID is the ranking model number in use (0 when no model is used).
	ModelID int64 `json:"model_id"`
	// SampleCutoff is the time up to which training/feedback samples were
	// observed when the generation was produced.
	SampleCutoff time.Time `json:"sample_cutoff"`
	// FallbackChain is the fallback recommender chain, in order.
	FallbackChain []string `json:"fallback_chain"`
}

// Fingerprint returns the stable, comparable identity of the components.
// The encoding is deterministic: map keys are sorted by encoding/json and
// timestamps are rendered in a fixed UTC layout.
func (c Components) Fingerprint() string {
	hash := md5.New()
	hash.Write([]byte(c.RecallDigest))
	hash.Write([]byte{0})
	hash.Write([]byte(c.RankerType))
	hash.Write([]byte{0})
	hash.Write([]byte(c.RankerDigest))
	hash.Write([]byte{0})
	hash.Write([]byte(strconv.FormatInt(c.ModelID, 10)))
	hash.Write([]byte{0})
	hash.Write([]byte(c.SampleCutoff.UTC().Format(time.RFC3339Nano)))
	hash.Write([]byte{0})
	// fallback chain order is semantically meaningful, do not sort it.
	hash.Write([]byte(strings.Join(c.FallbackChain, ",")))
	return hex.EncodeToString(hash.Sum(nil))
}

// Equal reports whether two component sets are identical, and therefore
// whether two generations must share the same version.
func (c Components) Equal(other Components) bool {
	return c.Fingerprint() == other.Fingerprint()
}

// Version is a registered generation version.
type Version struct {
	// Sequence is the monotonic version number, assigned at registration.
	Sequence int64 `json:"sequence"`
	// Fingerprint is the comparable identity of Components.
	Fingerprint string `json:"fingerprint"`
	// Components is the human-readable, secret-free component description.
	Components Components `json:"components"`
	// CreatedAt is when the version was first registered.
	CreatedAt time.Time `json:"created_at"`
}

// ComponentsFromConfig builds the version components from a recommend
// configuration, the ranking model number and the sample cutoff time.
// Ranker API tokens are deliberately excluded from the digest.
func ComponentsFromConfig(cfg *config.RecommendConfig, modelID int64, sampleCutoff time.Time) Components {
	return Components{
		RecallDigest:  cfg.Hash(),
		RankerType:    strings.ToLower(cfg.Ranker.Type),
		RankerDigest:  rankerFingerprint(cfg),
		ModelID:       modelID,
		SampleCutoff:  sampleCutoff.UTC(),
		FallbackChain: append([]string(nil), cfg.Fallback.Recommenders...),
	}
}

// rankerFingerprint hashes the ranker type and its non-secret parameters.
func rankerFingerprint(cfg *config.RecommendConfig) string {
	parts := []string{
		strings.ToLower(cfg.Ranker.Type),
		util.MD5(cfg.Ranker.Recommenders...),
		strconv.FormatInt(cfg.Ranker.FitPeriod.Nanoseconds(), 10),
		strconv.Itoa(cfg.Ranker.FitEpoch),
	}
	// Only non-secret fields of external rankers are included.
	parts = append(parts,
		cfg.Ranker.RerankerAPI.URL,
		cfg.Ranker.RerankerAPI.Model,
		cfg.Ranker.DecisionAPI.URL,
		cfg.Ranker.DecisionAPI.Model,
	)
	return util.MD5(parts...)
}

// marshalJSON serializes a value deterministically.
func marshalJSON(v any) (string, error) {
	buf, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

// unmarshalJSON parses a JSON string into v. An empty payload is reported as
// a corrupt-payload error rather than silently producing a zero value.
func unmarshalJSON(payload string, v any) error {
	if strings.TrimSpace(payload) == "" {
		return errCorruptVersionMeta
	}
	return json.Unmarshal([]byte(payload), v)
}

// sortedStrings returns a sorted copy of in, used to make test assertions and
// digest inputs order-independent where order carries no semantics.
func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

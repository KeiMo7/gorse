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
	"time"

	"github.com/gorse-io/gorse/logics"
)

// Explain is the additive, secret-free explanation attached to a recommend
// response. Existing response fields are untouched; old clients ignore it.
type Explain struct {
	// Version identifies the generation that produced the offline results.
	Version EffectiveVersion `json:"version"`
	// Freshness is the shared read verdict (fresh/stale/missing/rollback).
	Freshness Freshness `json:"freshness"`
	// Fallback reports whether and how the fallback chain was used.
	Fallback FallbackExplain `json:"fallback"`
	// Online reports request-time filtering and augmentation.
	Online OnlineExplain `json:"online"`
	// Sources maps each returned item id to its provenance label.
	Sources map[string]string `json:"sources,omitempty"`
}

// FallbackExplain describes fallback usage for a response.
type FallbackExplain struct {
	// Used is true when the offline result set was empty or insufficient.
	Used bool `json:"used"`
	// Chains lists the fallback recommenders that actually contributed.
	Chains []string `json:"chains,omitempty"`
	// OriginalVersionExisted reports whether an offline version existed before
	// fallback, even if its result set was empty.
	OriginalVersionExisted bool `json:"original_version_existed"`
}

// OnlineExplain reports request-time feedback effects.
type OnlineExplain struct {
	// Boundary is the latest feedback timestamp observed at request time.
	Boundary *time.Time `json:"boundary,omitempty"`
	// SampleCutoff is the offline generation's sample cutoff timestamp.
	SampleCutoff *time.Time `json:"sample_cutoff,omitempty"`
	// FilteredCount is how many offline items were removed at request time
	// because feedback arrived after the sample cutoff.
	FilteredCount int `json:"filtered_count"`
	// SupplementCount is how many items were added by request-time
	// augmentation (fallback chains and the latest online recommender).
	SupplementCount int `json:"supplement_count"`
}

// BuildExplain assembles the explanation from a resolver result and the
// recommender's provenance tracker. It never includes feedback text, item
// label text or secrets.
func BuildExplain(
	resolved *ResolveResult,
	rec *logics.Recommender,
	sampleCutoff time.Time,
) Explain {
	out := Explain{
		Version:   resolved.Effective,
		Freshness: resolved.Freshness,
		Sources:   make(map[string]string),
	}

	chainUsage := rec.ChainUsage()
	fallbackChains := make([]string, 0)
	supplement := 0
	for _, usage := range chainUsage {
		// The offline source is the primary result, not a fallback chain.
		if usage.Source == logics.SourceOffline {
			continue
		}
		fallbackChains = append(fallbackChains, usage.Source)
		supplement += usage.Count
	}
	out.Fallback = FallbackExplain{
		Used:                   len(fallbackChains) > 0,
		Chains:                 fallbackChains,
		OriginalVersionExisted: resolved.Pointer != nil,
	}

	// The online boundary is the newest feedback timestamp the recommender saw.
	if boundary := rec.LatestFeedbackTime(); !boundary.IsZero() {
		b := boundary.UTC()
		out.Online.Boundary = &b
	}
	if !sampleCutoff.IsZero() {
		c := sampleCutoff.UTC()
		out.Online.SampleCutoff = &c
	}
	out.Online.FilteredCount = rec.OfflineFiltered()
	out.Online.SupplementCount = supplement

	// Only record sources for items that made the final response.
	for _, score := range rec.ExplainedScores() {
		if source := rec.Provenance(score.Id); source != "" {
			out.Sources[score.Id] = source
		}
	}
	return out
}

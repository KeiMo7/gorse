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
	"fmt"
	"testing"
	"time"

	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	client, err := cache.Open(fmt.Sprintf("sqlite://%s/cache.db", t.TempDir()), "")
	require.NoError(t, err)
	require.NoError(t, client.Init())
	t.Cleanup(func() { _ = client.Close() })
	clock := func() time.Time { return time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC) }
	return NewStore(client, 3).WithClock(clock)
}

func baseComponents() Components {
	return Components{
		RecallDigest:  "recall-1",
		RankerType:    "fm",
		RankerDigest:  "ranker-1",
		ModelID:       42,
		SampleCutoff:  time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC),
		FallbackChain: []string{"item-to-item/neighbors", "latest"},
	}
}

func TestComponentsFingerprintEquivalentAndChange(t *testing.T) {
	// Equivalent re-runs are identical even when timestamps carry different zones.
	a := baseComponents()
	b := baseComponents()
	b.SampleCutoff = a.SampleCutoff.In(time.FixedZone("east", 8*3600))
	assert.Equal(t, a.Fingerprint(), b.Fingerprint())
	assert.True(t, a.Equal(b))

	// Every single component change must produce a new fingerprint.
	cases := map[string]func(c *Components){
		"recall digest":     func(c *Components) { c.RecallDigest = "recall-2" },
		"ranker type":       func(c *Components) { c.RankerType = "reranker" },
		"ranker digest":     func(c *Components) { c.RankerDigest = "ranker-2" },
		"model id":          func(c *Components) { c.ModelID = 43 },
		"sample cutoff":     func(c *Components) { c.SampleCutoff = c.SampleCutoff.Add(time.Minute) },
		"fallback chain":    func(c *Components) { c.FallbackChain = []string{"latest"} },
		"fallback ordering": func(c *Components) { c.FallbackChain = []string{"latest", "item-to-item/neighbors"} },
	}
	base := baseComponents().Fingerprint()
	for name, mutate := range cases {
		changed := baseComponents()
		mutate(&changed)
		assert.NotEqual(t, base, changed.Fingerprint(), "change not detected: %s", name)
	}
}

func TestComponentsFromConfigIsSecretFree(t *testing.T) {
	cfg := config.GetDefaultConfig()
	cfg.Recommend.Ranker.Type = "reranker"
	cfg.Recommend.Ranker.RerankerAPI.URL = "https://example.test/rank"
	cfg.Recommend.Ranker.RerankerAPI.AuthToken = "super-secret-token"
	cfg.Recommend.Ranker.RerankerAPI.Model = "rank-model"
	cfg.Recommend.Fallback.Recommenders = []string{"latest"}

	cutoff := time.Date(2026, 11, 25, 9, 0, 0, 0, time.UTC)
	comp := ComponentsFromConfig(&cfg.Recommend, 7, cutoff)
	payload := fmt.Sprintf("%v", comp)
	assert.NotContains(t, payload, "super-secret-token")
	assert.Equal(t, "reranker", comp.RankerType)
	assert.Equal(t, int64(7), comp.ModelID)
	assert.Equal(t, []string{"latest"}, comp.FallbackChain)

	// Same inputs produce the same digest.
	again := ComponentsFromConfig(&cfg.Recommend, 7, cutoff)
	assert.Equal(t, comp.Fingerprint(), again.Fingerprint())
}

func TestRegistryEquivalentRerunAndSequence(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	comp := baseComponents()
	v1, created, err := store.GetOrRegisterVersion(ctx, comp)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, int64(1), v1.Sequence)

	// Equivalent re-run returns the same version, no new sequence.
	same, created, err := store.GetOrRegisterVersion(ctx, comp)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, v1.Sequence, same.Sequence)
	assert.Equal(t, v1.Fingerprint, same.Fingerprint)

	// A changed component creates the next sequence.
	comp2 := baseComponents()
	comp2.ModelID = 43
	v2, created, err := store.GetOrRegisterVersion(ctx, comp2)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, int64(2), v2.Sequence)

	versions, err := store.Versions(ctx)
	require.NoError(t, err)
	require.Len(t, versions, 2)
	assert.Equal(t, int64(1), versions[0].Sequence)
	assert.Equal(t, int64(2), versions[1].Sequence)

	// Current version round-trips.
	require.NoError(t, store.SetCurrentVersion(ctx, v2))
	cur, err := store.CurrentVersion(ctx)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.Equal(t, int64(2), cur.Sequence)
}

func TestSubsetRoundTrip(t *testing.T) {
	subset := Subset("user/with:id", 9)
	userId, seq, ok := parseSubset(subset)
	require.True(t, ok)
	assert.Equal(t, "user/with:id", userId)
	assert.Equal(t, int64(9), seq)

	for _, bad := range []string{"", "noseparator", "#12", "user#", "user#abc"} {
		_, _, ok := parseSubset(bad)
		assert.False(t, ok, "expected malformed subset: %q", bad)
	}
}

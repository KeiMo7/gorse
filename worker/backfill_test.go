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

package worker

import (
	"time"

	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/gorse-io/gorse/versioning"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackfillGeneratorCommitsAtTarget verifies the real pipeline generator
// recomputes one user and commits at an explicit target version, leaving an
// unselected user untouched.
func (suite *WorkerTestSuite) TestBackfillGeneratorCommitsAtTarget() {
	ctx := suite.T().Context()
	// Seed a user with positive feedback and items so recall produces results.
	now := time.Now()
	require.NoError(suite.T(), suite.DataClient.BatchInsertUsers(ctx, []data.User{{UserId: "bf1"}, {UserId: "bf2"}}))
	require.NoError(suite.T(), suite.DataClient.BatchInsertItems(ctx, []data.Item{
		{ItemId: "i1", Timestamp: now},
		{ItemId: "i2", Timestamp: now},
		{ItemId: "i3", Timestamp: now},
	}))
	require.NoError(suite.T(), suite.DataClient.BatchInsertFeedback(ctx, []data.Feedback{
		{FeedbackType: "click", UserId: "bf1", ItemId: "i1", Timestamp: now.Add(-time.Hour)},
	}, true, true, true))

	// Register two versions; backfill targets v2 while the user starts on v1.
	store := versioning.NewStore(suite.CacheClient, 3)
	comp1 := versioning.Components{RecallDigest: "r1", RankerType: "none", RankerDigest: "k1",
		SampleCutoff: now.Add(-2 * time.Hour).UTC(), FallbackChain: []string{"latest"}}
	v1, _, err := store.GetOrRegisterVersion(ctx, comp1)
	require.NoError(suite.T(), err)
	require.NoError(suite.T(), store.SetCurrentVersion(ctx, v1))
	require.NoError(suite.T(), store.CommitResult(ctx, "bf1", v1,
		[]cache.Score{{Id: "i1", Score: 1}}, true, "d"))

	comp2 := versioning.Components{RecallDigest: "r2", RankerType: "none", RankerDigest: "k2",
		SampleCutoff: now.UTC(), FallbackChain: []string{"latest"}}
	v2, _, err := store.GetOrRegisterVersion(ctx, comp2)
	require.NoError(suite.T(), err)

	gen := NewBackfillGenerator(&suite.Pipeline)
	require.NoError(suite.T(), gen.GenerateForUser(ctx, "bf1", v2))

	// The user pointer now references v2 and a v2 subset exists.
	pointer, err := store.ReadUserPointer(ctx, "bf1")
	require.NoError(suite.T(), err)
	require.NotNil(suite.T(), pointer)
	assert.Equal(suite.T(), v2.Sequence, pointer.Sequence)
	ok, err := store.HasVersionedResult(ctx, "bf1", v2.Sequence)
	require.NoError(suite.T(), err)
	assert.True(suite.T(), ok)

	// The unselected user bf2 was never written.
	pointer2, err := store.ReadUserPointer(ctx, "bf2")
	require.NoError(suite.T(), err)
	assert.Nil(suite.T(), pointer2)
}

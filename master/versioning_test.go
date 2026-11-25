// Copyright 2026 gorse Project authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package master

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/gorse-io/gorse/versioning"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (suite *MasterAPITestSuite) seedVersioningFixture(t *testing.T) (versioning.Version, versioning.Version) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, suite.DataClient.BatchInsertUsers(ctx, []data.User{
		{UserId: "u1"}, {UserId: "u2"}, {UserId: "u3"},
	}))
	store := versioning.NewStore(suite.CacheClient, 3)
	// A test-unique salt keeps fingerprints distinct across tests even though
	// the cache is shared, so version sequences stay deterministic.
	salt := t.Name()
	v1, _, err := store.GetOrRegisterVersion(ctx, versioning.Components{
		RecallDigest: "r1-" + salt, RankerType: "none", RankerDigest: "k1",
		SampleCutoff: time.Now().Add(-time.Hour).UTC(), FallbackChain: []string{"latest"},
	})
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentVersion(ctx, v1))
	require.NoError(t, store.CommitResult(ctx, "u1", v1,
		[]cache.Score{{Id: "i1", Score: 2}}, true, "d"))
	require.NoError(t, store.CommitResult(ctx, "u2", v1,
		[]cache.Score{{Id: "i2", Score: 1}}, true, "d"))

	comp2 := versioning.Components{
		RecallDigest: "r2-" + salt, RankerType: "none", RankerDigest: "k2",
		SampleCutoff: time.Now().UTC(), FallbackChain: []string{"latest"},
	}
	v2, _, err := store.GetOrRegisterVersion(ctx, comp2)
	require.NoError(t, err)
	require.NoError(t, store.SetCurrentVersion(ctx, v2))
	// u3 is current; u1/u2 remain on v1.
	require.NoError(t, store.CommitResult(ctx, "u3", v2,
		[]cache.Score{{Id: "i3", Score: 1}}, true, "d"))
	return v1, v2
}

// doRequest issues an authenticated dashboard request and returns the recorder.
func (suite *MasterAPITestSuite) doRequest(method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Cookie", suite.cookie)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	suite.handler.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var out T
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	return out
}

func (suite *MasterAPITestSuite) TestVersioningDryRunAndObserveAPI() {
	v1, v2 := suite.seedVersioningFixture(suite.T())

	// Dry-run backfill for users behind v2: candidates reported, no writes.
	body := `{"selector":{"scope":"behind_version","behind_sequence":` +
		itoa(v2.Sequence) + `},"dry_run":true}`
	rec := suite.doRequest(http.MethodPost, "/api/dashboard/backfills", body)
	require.Equal(suite.T(), http.StatusOK, rec.Code, rec.Body.String())
	dry := decode[versioning.Task](suite.T(), rec)
	require.True(suite.T(), dry.DryRun)
	assert.Equal(suite.T(), 2, dry.Total, "u1 and u2 are behind v2")
	assert.Equal(suite.T(), map[int64]int{v1.Sequence: 2}, dry.VersionDistribution)

	// Observability endpoint groups counters by version and reports tasks.
	rec = suite.doRequest(http.MethodGet, "/api/dashboard/versions/observe", "")
	require.Equal(suite.T(), http.StatusOK, rec.Code, rec.Body.String())
	snapshot := decode[versioning.ObservabilitySnapshot](suite.T(), rec)
	assert.Equal(suite.T(), v2.Sequence, snapshot.CurrentSequence)
	assert.Equal(suite.T(), 3, snapshot.Totals.Total)
	assert.Equal(suite.T(), 1, snapshot.Totals.Fresh)
	assert.Equal(suite.T(), 2, snapshot.Totals.Stale)
	require.NotEmpty(suite.T(), snapshot.LatestTasks)
}

func (suite *MasterAPITestSuite) TestVersioningRollbackRejectAndSwitchAPI() {
	v1, _ := suite.seedVersioningFixture(suite.T())
	ctx := suite.T().Context()
	// A fresh user that never received v1 results: global rollback must reject.
	require.NoError(suite.T(), suite.DataClient.BatchInsertUsers(ctx, []data.User{{UserId: "u-no-v1"}}))

	// Global rollback to v1 must reject: not every user retains v1 rows; the
	// stored rollback state must remain unchanged.
	rec := suite.doRequest(http.MethodPost, "/api/dashboard/rollback",
		`{"sequence":`+itoa(v1.Sequence)+`}`)
	assert.Equal(suite.T(), http.StatusBadRequest, rec.Code)
	store := versioning.NewStore(suite.CacheClient, 3)
	rb, err := store.Rollback(suite.T().Context())
	require.NoError(suite.T(), err)
	assert.Nil(suite.T(), rb, "rollback state must be unchanged after rejection")

	// Scoped rollback to u1/u2 (which retain v1) succeeds.
	rec = suite.doRequest(http.MethodPost, "/api/dashboard/rollback",
		`{"sequence":`+itoa(v1.Sequence)+`,"user_ids":["u1","u2"],"reason":"regression"}`)
	require.Equal(suite.T(), http.StatusOK, rec.Code, rec.Body.String())
	var activated struct {
		State   *versioning.RollbackState `json:"state"`
		Missing []versioning.MissingUser  `json:"missing"`
	}
	require.NoError(suite.T(), json.Unmarshal(rec.Body.Bytes(), &activated))
	require.NotNil(suite.T(), activated.State)
	assert.Empty(suite.T(), activated.Missing)

	// Versions list contains both generations.
	rec = suite.doRequest(http.MethodGet, "/api/dashboard/versions", "")
	require.Equal(suite.T(), http.StatusOK, rec.Code)
	versions := decode[[]versioning.Version](suite.T(), rec)
	assert.Len(suite.T(), versions, 2)

	// Clear the rollback.
	rec = suite.doRequest(http.MethodDelete, "/api/dashboard/rollback", "")
	assert.Equal(suite.T(), http.StatusOK, rec.Code)
}

func (suite *MasterAPITestSuite) TestBackfillExplicitUsersTaskLifecycle() {
	_, v2 := suite.seedVersioningFixture(suite.T())

	body := `{"selector":{"scope":"users","user_ids":["u1"]},"target_sequence":` +
		itoa(v2.Sequence) + `,"concurrency":1}`
	rec := suite.doRequest(http.MethodPost, "/api/dashboard/backfills", body)
	require.Equal(suite.T(), http.StatusOK, rec.Code, rec.Body.String())
	task := decode[versioning.Task](suite.T(), rec)
	assert.Equal(suite.T(), v2.Sequence, task.Target.Sequence)

	// Poll until terminal.
	require.Eventually(suite.T(), func() bool {
		got := suite.doRequest(http.MethodGet, "/api/dashboard/backfills/"+task.ID, "")
		var t versioning.Task
		if err := json.Unmarshal(got.Body.Bytes(), &t); err != nil {
			return false
		}
		return t.Status == versioning.TaskCompleted || t.Status == versioning.TaskFailed
	}, 5*time.Second, 20*time.Millisecond)

	// Selected user moves to v2; unselected u2 stays behind.
	store := versioning.NewStore(suite.CacheClient, 3)
	p1, err := store.ReadUserPointer(suite.T().Context(), "u1")
	require.NoError(suite.T(), err)
	assert.Equal(suite.T(), v2.Sequence, p1.Sequence)
	p2, err := store.ReadUserPointer(suite.T().Context(), "u2")
	require.NoError(suite.T(), err)
	assert.Less(suite.T(), p2.Sequence, v2.Sequence, "unselected user must not change")
}

// itoa avoids pulling strconv into the import block more than once.
func itoa(v int64) string {
	return strings.TrimSpace(jsonNumber(v))
}

func jsonNumber(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}

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

package master

import (
	"context"
	"net/http"

	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/gorse-io/gorse/server"
	"github.com/gorse-io/gorse/versioning"
	"github.com/gorse-io/gorse/worker"
)

// VersionManager returns a manager bound to the master's current shared stores.
// The manager is rebuilt whenever the cache/data client is replaced (which
// happens during meta sync and in tests), so it never holds a stale handle.
// The same cache store backs online reads, so backfill and rollback
// concurrency is bounded by config and cannot starve serving.
func (m *Master) VersionManager() (*versioning.Manager, error) {
	m.versionManagerMu.Lock()
	defer m.versionManagerMu.Unlock()
	if m.versionManager != nil && m.versionManagerCache == m.CacheClient && m.versionManagerData == m.DataClient {
		return m.versionManager, nil
	}
	store := versioning.NewStore(m.CacheClient, m.Config.Versioning.RetainedVersions)
	backing := versioning.NewCacheBackfillStore(m.CacheClient, m.DataClient)
	m.versionManager = versioning.NewManager(store, backing)
	m.versionManagerCache = m.CacheClient
	m.versionManagerData = m.DataClient
	return m.versionManager, nil
}

// backfillRequest is the dashboard payload for starting a backfill.
type backfillRequest struct {
	Selector       versioning.Selector `json:"selector"`
	DryRun         bool                `json:"dry_run"`
	Concurrency    int                 `json:"concurrency"`
	MaxFailures    int                 `json:"max_failures"`
	TargetSequence int64               `json:"target_sequence"`
}

// registerVersioningRoutes attaches the version management endpoints. They
// live under /api/dashboard and reuse the existing dashboard auth filter.
func (m *Master) registerVersioningRoutes(ws *restful.WebService) {
	ws.Route(ws.GET("/dashboard/versions").To(m.listVersions).
		Doc("List registered recommendation versions.").
		Metadata(restfulspec.KeyOpenAPITags, []string{"dashboard"}).
		Returns(http.StatusOK, "OK", []versioning.Version{}))
	ws.Route(ws.GET("/dashboard/versions/observe").To(m.observeVersions).
		Doc("Freshness, rollback and data-age counters grouped by version.").
		Metadata(restfulspec.KeyOpenAPITags, []string{"dashboard"}).
		Returns(http.StatusOK, "OK", versioning.ObservabilitySnapshot{}))
	ws.Route(ws.POST("/dashboard/backfills").To(m.startBackfill).
		Doc("Start or dry-run a targeted recommendation backfill.").
		Metadata(restfulspec.KeyOpenAPITags, []string{"dashboard"}).
		Reads(backfillRequest{}).
		Returns(http.StatusOK, "OK", versioning.Task{}))
	ws.Route(ws.GET("/dashboard/backfills").To(m.listBackfills).
		Doc("List backfill tasks.").
		Metadata(restfulspec.KeyOpenAPITags, []string{"dashboard"}).
		Returns(http.StatusOK, "OK", []*versioning.Task{}))
	ws.Route(ws.GET("/dashboard/backfills/{task-id}").To(m.getBackfill).
		Doc("Get one backfill task.").
		Metadata(restfulspec.KeyOpenAPITags, []string{"dashboard"}).
		Returns(http.StatusOK, "OK", versioning.Task{}))
	ws.Route(ws.POST("/dashboard/backfills/{task-id}/cancel").To(m.cancelBackfill).
		Doc("Cancel a running backfill; progress is retained for resume.").
		Metadata(restfulspec.KeyOpenAPITags, []string{"dashboard"}).
		Returns(http.StatusOK, "OK", versioning.Task{}))
	ws.Route(ws.POST("/dashboard/backfills/{task-id}/resume").To(m.resumeBackfill).
		Doc("Resume an interrupted or partially failed backfill.").
		Metadata(restfulspec.KeyOpenAPITags, []string{"dashboard"}).
		Returns(http.StatusOK, "OK", versioning.Task{}))
	ws.Route(ws.POST("/dashboard/rollback").To(m.activateRollback).
		Doc("Roll online reads back to a previous version.").
		Metadata(restfulspec.KeyOpenAPITags, []string{"dashboard"}).
		Reads(versioning.RollbackRequest{}).
		Returns(http.StatusOK, "OK", versioning.RollbackState{}))
	ws.Route(ws.DELETE("/dashboard/rollback").To(m.deactivateRollback).
		Doc("Clear a rollback and resume serving the current version.").
		Metadata(restfulspec.KeyOpenAPITags, []string{"dashboard"}).
		Returns(http.StatusOK, "OK", server.Success{}))
}

func (m *Master) listVersions(request *restful.Request, response *restful.Response) {
	mgr, err := m.VersionManager()
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	versions, err := mgr.Store().Versions(request.Request.Context())
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	server.Ok(response, versions)
}

func (m *Master) observeVersions(request *restful.Request, response *restful.Response) {
	mgr, err := m.VersionManager()
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	snapshot, err := mgr.Observe(request.Request.Context(), m.Config.Recommend.CacheExpire, 20)
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	server.Ok(response, snapshot)
}

func (m *Master) startBackfill(request *restful.Request, response *restful.Response) {
	var req backfillRequest
	if err := request.ReadEntity(&req); err != nil {
		server.BadRequest(response, err)
		return
	}
	mgr, err := m.VersionManager()
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	ctx := request.Request.Context()
	target, err := m.resolveBackfillTarget(ctx, mgr, req.TargetSequence)
	if err != nil {
		server.BadRequest(response, err)
		return
	}
	concurrency := req.Concurrency
	if concurrency <= 0 {
		concurrency = m.Config.Versioning.BackfillConcurrency
	}
	maxFailures := req.MaxFailures
	if maxFailures <= 0 {
		maxFailures = m.Config.Versioning.MaxBackfillFailures
	}
	var gen versioning.Generator
	if !req.DryRun {
		gen = worker.NewBackfillGenerator(m.backfillPipeline())
	}
	task, err := mgr.StartBackfill(ctx, req.Selector, *target, req.DryRun, concurrency, maxFailures, gen)
	if err != nil {
		server.BadRequest(response, err)
		return
	}
	server.Ok(response, task)
}

// resolveBackfillTarget returns the requested version, defaulting to the
// current version when targetSequence is zero.
func (m *Master) resolveBackfillTarget(ctx context.Context, mgr *versioning.Manager, seq int64) (*versioning.Version, error) {
	if seq == 0 {
		current, err := mgr.Store().CurrentVersion(ctx)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, versioning.ErrNoCurrentVersion
		}
		return current, nil
	}
	return mgr.Store().VersionBySequence(ctx, seq)
}

// backfillPipeline builds a pipeline carrying the master's shared stores.
// Trained models are installed by the same load path as the periodic
// updateRecommend task before a backfill is invoked; the pipeline degrades
// gracefully to model-free ranking when a model is not resident.
func (m *Master) backfillPipeline() *worker.Pipeline {
	return &worker.Pipeline{
		Config:       m.Config,
		DataClient:   m.DataClient,
		CacheClient:  m.CacheClient,
		VectorClient: m.VectorClient,
		Tracer:       m.tracer,
		Jobs:         m.Config.Master.NumJobs,
	}
}

func (m *Master) listBackfills(request *restful.Request, response *restful.Response) {
	mgr, err := m.VersionManager()
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	tasks, err := mgr.ListTasks(request.Request.Context())
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	server.Ok(response, tasks)
}

func (m *Master) getBackfill(request *restful.Request, response *restful.Response) {
	mgr, err := m.VersionManager()
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	task, err := mgr.GetTask(request.Request.Context(), request.PathParameter("task-id"))
	if err != nil {
		server.PageNotFound(response, err)
		return
	}
	server.Ok(response, task)
}

func (m *Master) cancelBackfill(request *restful.Request, response *restful.Response) {
	mgr, err := m.VersionManager()
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	if err := mgr.Cancel(request.Request.Context(), request.PathParameter("task-id")); err != nil {
		server.BadRequest(response, err)
		return
	}
	task, err := mgr.GetTask(request.Request.Context(), request.PathParameter("task-id"))
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	server.Ok(response, task)
}

func (m *Master) resumeBackfill(request *restful.Request, response *restful.Response) {
	mgr, err := m.VersionManager()
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	gen := worker.NewBackfillGenerator(m.backfillPipeline())
	task, err := mgr.Resume(request.Request.Context(), request.PathParameter("task-id"), gen)
	if err != nil {
		server.BadRequest(response, err)
		return
	}
	server.Ok(response, task)
}

func (m *Master) activateRollback(request *restful.Request, response *restful.Response) {
	var req versioning.RollbackRequest
	if err := request.ReadEntity(&req); err != nil {
		server.BadRequest(response, err)
		return
	}
	mgr, err := m.VersionManager()
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	state, missing, err := mgr.ActivateRollback(request.Request.Context(), req)
	if err != nil {
		server.BadRequest(response, err)
		return
	}
	server.Ok(response, map[string]any{"state": state, "missing": missing})
}

func (m *Master) deactivateRollback(request *restful.Request, response *restful.Response) {
	mgr, err := m.VersionManager()
	if err != nil {
		server.InternalServerError(response, err)
		return
	}
	var userIDs []string
	if err := request.ReadEntity(&userIDs); err != nil {
		userIDs = nil
	}
	if err := mgr.DeactivateRollback(request.Request.Context(), userIDs); err != nil {
		server.InternalServerError(response, err)
		return
	}
	server.Ok(response, server.Success{RowAffected: 1})
}

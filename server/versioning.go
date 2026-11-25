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

package server

import (
	"context"
	"strconv"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/gorse-io/gorse/common/log"
	"github.com/gorse-io/gorse/logics"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/versioning"
	"go.uber.org/zap"
)

// versioningBox lazily builds the versioning store/resolver from the current
// cache client. It is rebuilt when the client is replaced during meta sync.
type versioningBox struct {
	store    *versioning.Store
	resolver *versioning.Resolver
	client   cache.Database
}

func (s *RestServer) versioning() *versioningBox {
	s.versionMu.Lock()
	defer s.versionMu.Unlock()
	if s.versionBox != nil && s.versionBox.client == s.CacheClient {
		return s.versionBox
	}
	store := versioning.NewStore(s.CacheClient, s.Config.Versioning.RetainedVersions)
	s.versionBox = &versioningBox{
		store:    store,
		resolver: versioning.NewResolver(store, s.Config.Recommend.CacheExpire),
		client:   s.CacheClient,
	}
	return s.versionBox
}

// recommendExplanation is the additive JSON body of the explain endpoint. The
// original recommendation list is carried alongside the explanation.
type recommendExplanation struct {
	Items       any                `json:"items"`
	Explanation versioning.Explain `json:"explanation"`
}

// getRecommendExplain serves the same recommendation as getRecommend but with
// version, freshness, fallback and online-augmentation explanation attached.
// It never changes the legacy endpoint's path, fields, order, count, paging or
// status codes.
func (s *RestServer) getRecommendExplain(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	if request != nil && request.Request != nil {
		ctx = request.Request.Context()
	}
	userId := request.PathParameter("user-id")
	n, err := ParseInt(request, "n", s.Config.Server.DefaultN)
	if err != nil {
		BadRequest(response, err)
		return
	}
	categories := ReadCategories(request, nil)
	offset, err := ParseInt(request, "offset", 0)
	if err != nil {
		BadRequest(response, err)
		return
	}

	box := s.versioning()
	// Fetch only the requested window plus offset so an offline set that fully
	// satisfies the request is not pointlessly enlarged, which would trigger the
	// fallback chain and change the observable provenance.
	resolved, err := box.resolver.Resolve(ctx, userId, categories, 0, n+offset)
	if err != nil {
		// Version storage trouble must not fail the request: fall back to the
		// legacy recommender path without explanation.
		log.ResponseLogger(response).Warn("version resolve failed, falling back to legacy read", zap.Error(err))
		s.getRecommend(request, response)
		return
	}

	recommender, err := logics.NewRecommender(s.Config.Recommend, s.CacheClient, s.DataClient, s.VectorClient, true, userId, categories)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	recommender.EnableExplain()

	// Feed the versioned offline rows through the same exclusion/fallback
	// machinery as the standard path, preserving content, order and count.
	scores, err := recommender.RecommendFromOffline(ctx, resolved.Scores, n+offset)
	if err != nil {
		InternalServerError(response, err)
		return
	}
	if len(scores) > offset {
		scores = scores[offset:]
	} else {
		scores = []cache.Score{}
	}
	recommender.RememberFinal(scores)

	// Sample cutoff comes from the effective registered version, when known.
	var sampleCutoff time.Time
	if v, vErr := box.store.VersionBySequence(ctx, resolved.Effective.Sequence); vErr == nil && v != nil {
		sampleCutoff = v.Components.SampleCutoff
	}
	explain := versioning.BuildExplain(resolved, recommender, sampleCutoff)

	var items any = scores
	if apiVersion := request.HeaderParameter("X-API-Version"); apiVersion != "2" {
		ids := make([]string, 0, len(scores))
		for _, sc := range scores {
			ids = append(ids, sc.Id)
		}
		items = ids
	}

	// Additive headers on the response; old clients simply ignore them.
	response.AddHeader("X-Gorse-Version-Sequence", strconv.FormatInt(resolved.Effective.Sequence, 10))
	response.AddHeader("X-Gorse-Freshness", resolved.Freshness.State)
	if resolved.Effective.Rollback {
		response.AddHeader("X-Gorse-Rollback", "true")
	}
	Ok(response, recommendExplanation{Items: items, Explanation: explain})
}

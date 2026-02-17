//
// Copyright 2026 gRPC authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef GRPC_SRC_CORE_CREDENTIALS_CALL_REGIONAL_ACCESS_BOUNDARY_FETCHER_H
#define GRPC_SRC_CORE_CREDENTIALS_CALL_REGIONAL_ACCESS_BOUNDARY_FETCHER_H

#include <grpc/event_engine/event_engine.h>
#include <grpc/support/port_platform.h>
#include <grpc/support/sync.h>
#include <grpc/support/time.h>

#include <optional>
#include <string>
#include <vector>

#include "src/core/call/metadata.h"
#include "src/core/lib/iomgr/error.h"
#include "src/core/util/backoff.h"
#include "src/core/util/http_client/httpcli.h"
#include "src/core/util/orphanable.h"
#include "src/core/util/ref_counted_ptr.h"
#include "absl/status/statusor.h"

namespace grpc_core {

constexpr Duration kRegionalAccessBoundaryCacheDuration = Duration::Hours(6);
constexpr Duration kRegioanlAccessBoundarySoftCacheGraceDuration =
    Duration::Hours(1);

struct RegionalAccessBoundary {
  std::string encoded_locations;
  std::vector<std::string> locations;
  Timestamp expiration =
      Timestamp::Now() + kRegionalAccessBoundaryCacheDuration;

  bool isValid() const { return expiration > Timestamp::Now(); }

  bool IsSoftExpired() const {
    return (expiration - kRegioanlAccessBoundarySoftCacheGraceDuration) <
           Timestamp::Now();
  }
};

class RegionalAccessBoundaryFetcher final
    : public InternallyRefCounted<RegionalAccessBoundaryFetcher> {
 public:
  friend class RegionalAccessBoundaryFetcherTest;

  RegionalAccessBoundaryFetcher();
  // Attaches regional access boundary header (x-allowed-locations) to the
  // initial metadata if available, otherwise initiates non-blocking,
  // asynchronous fetch of regional access boundary if not already cached or in
  // flight.
  void Fetch(absl::string_view lookup_url, absl::string_view access_token,
             ClientMetadata& initial_metadata);

  // Cancels any pending fetch of regional access boundary which must be called
  // during destruction of any CallCredential which supports regional access
  // boundary to avoid memory leaks from pending http requests.
  void Orphan() override;

 private:
  class RegionalAccessBoundaryRequest;

  void StartRegionalAccessBoundaryFetch(
      RefCountedPtr<RegionalAccessBoundaryRequest> req);

  void OnRegionalAccessBoundaryResponse(
      RefCountedPtr<RegionalAccessBoundaryRequest> req,
      grpc_error_handle error);
  static void OnRegionalAccessBoundaryResponseWrapper(void* arg,
                                                      grpc_error_handle error);
  void RetryFetchRegionalAccessBoundary(RegionalAccessBoundaryRequest* req,
                                        grpc_error_handle error);

  std::shared_ptr<grpc_event_engine::experimental::EventEngine> event_engine_ =
      grpc_event_engine::experimental::GetDefaultEventEngine();
  Mutex cache_mu_;
  std::optional<grpc_event_engine::experimental::EventEngine::TaskHandle>
      retry_timer_handle_ ABSL_GUARDED_BY(&cache_mu_);
  std::optional<RegionalAccessBoundary> cache_ ABSL_GUARDED_BY(&cache_mu_);
  bool fetch_in_flight_ ABSL_GUARDED_BY(&cache_mu_) = false;
  int cooldown_multiplier_ ABSL_GUARDED_BY(&cache_mu_) = 1;
  Timestamp cooldown_deadline_ ABSL_GUARDED_BY(&cache_mu_) =
      Timestamp::ProcessEpoch();
  BackOff backoff_ ABSL_GUARDED_BY(&cache_mu_);
  RefCountedPtr<RegionalAccessBoundaryRequest> pending_request_
      ABSL_GUARDED_BY(&cache_mu_);
};

class RegionalAccessBoundaryFetcher::RegionalAccessBoundaryRequest final
    : public RefCounted<RegionalAccessBoundaryRequest> {
 public:
  explicit RegionalAccessBoundaryRequest(URI uri,
                                         absl::string_view access_token);

  ~RegionalAccessBoundaryRequest() override {
    grpc_http_response_destroy(&response_);
    grpc_pollset_set_destroy(grpc_polling_entity_pollset_set(&pollent_));
  }

  void SwapPollent(RegionalAccessBoundaryRequest& other) {
    std::swap(pollent_, other.pollent_);
  }

 private:
  friend class RegionalAccessBoundaryFetcher;

  grpc_http_response response_;
  OrphanablePtr<HttpRequest> http_request_;
  std::string access_token_;
  URI uri_;
  grpc_polling_entity pollent_;
  int num_retries_ = 0;
  RefCountedPtr<RegionalAccessBoundaryFetcher> fetcher_;
  grpc_closure closure_;
};

}  // namespace grpc_core

#endif  // GRPC_SRC_CORE_CREDENTIALS_CALL_REGIONAL_ACCESS_BOUNDARY_FETCHER_H
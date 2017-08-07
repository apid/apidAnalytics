// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package apidAnalytics

import (
	"github.com/30x/apid-core"
	"net/http"
	"strings"
)

var analyticsBasePath string

type errResponse struct {
	ErrorCode string `json:"errorCode"`
	Reason    string `json:"reason"`
}

type dbError struct {
	ErrorCode string `json:"errorCode"`
	Reason    string `json:"reason"`
}

func initAPI(services apid.Services) {
	log.Debug("initialized API's exposed by apidAnalytics plugin")
	analyticsBasePath = config.GetString(configAnalyticsBasePath)
	services.API().HandleFunc(analyticsBasePath+"/{bundle_scope_uuid}",
		saveAnalyticsRecord).Methods("POST")
	services.API().HandleFunc(analyticsBasePath,
		processAnalyticsRecord).Methods("POST")
}

func saveAnalyticsRecord(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	db := getDB() // When database isnt initialized
	if db == nil {
		writeError(w, http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"Service is not initialized completely")
		return
	}

	if !strings.EqualFold(r.Header.Get("Content-Type"), "application/json") {
		writeError(w, http.StatusBadRequest, "UNSUPPORTED_CONTENT_TYPE",
			"Only supported content type is application/json")
		return
	}

	vars := apid.API().Vars(r)
	scopeuuid := vars["bundle_scope_uuid"]
	tenant, dbErr := getTenantForScope(scopeuuid)
	if dbErr.ErrorCode != "" {
		switch dbErr.ErrorCode {
		case "INTERNAL_SEARCH_ERROR":
			writeError(w, http.StatusInternalServerError,
				"INTERNAL_SEARCH_ERROR", dbErr.Reason)
		case "UNKNOWN_SCOPE":
			writeError(w, http.StatusBadRequest,
				"UNKNOWN_SCOPE", dbErr.Reason)
		}
	} else {
		body, err := getJsonBody(r)
		if err.ErrorCode == "" {
			err = validateEnrichPublish(tenant, body)
			if err.ErrorCode == "" {
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		writeError(w, http.StatusBadRequest, err.ErrorCode, err.Reason)
	}
}

func processAnalyticsRecord(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	db := getDB() // When database isnt initialized
	if db == nil {
		writeError(w, http.StatusInternalServerError,
			"INTERNAL_SERVER_ERROR",
			"Service is not initialized completely")
		return
	}

	if !strings.EqualFold(r.Header.Get("Content-Type"), "application/json") {
		writeError(w, http.StatusBadRequest, "UNSUPPORTED_CONTENT_TYPE",
			"Only supported content type is application/json")
		return
	}

	body, err := getJsonBody(r)
	if err.ErrorCode == "" {
		tenant, e := getTenantFromPayload(body)
		if e.ErrorCode == "" {
			_, dbErr := validateTenant(tenant)
			if dbErr.ErrorCode != "" {
				switch dbErr.ErrorCode {
				case "INTERNAL_SEARCH_ERROR":
					writeError(w, http.StatusInternalServerError,
						"INTERNAL_SEARCH_ERROR", dbErr.Reason)
				case "UNKNOWN_SCOPE":
					writeError(w, http.StatusBadRequest,
						"UNKNOWN_SCOPE", dbErr.Reason)
				}
				return
			} else {
				err = validateEnrichPublish(tenant, body)
				if err.ErrorCode == "" {
					w.WriteHeader(http.StatusOK)
					return
				}
			}
		} else {
			writeError(w, http.StatusBadRequest,
				e.ErrorCode, e.Reason)
			return
		}
	}
	writeError(w, http.StatusBadRequest, err.ErrorCode, err.Reason)
}

package apidAnalytics

import (
	"github.com/30x/apid"
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
		err := processPayload(tenant, scopeuuid, r)
		if err.ErrorCode == "" {
			w.WriteHeader(http.StatusOK)
		} else {
			writeError(w, http.StatusBadRequest,
				err.ErrorCode, err.Reason)
		}
	}
}

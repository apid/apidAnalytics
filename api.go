package apidAnalytics

import (
	"database/sql"
	"encoding/json"
	"github.com/30x/apid"
	"net/http"
)

var analyticsBasePath string

type errResponse struct {
	ErrorCode string `json:"errorCode"`
	Reason    string `json:"reason"`
}

type dbError struct {
	reason string
	errorCode    string
}

type tenant struct {
	org string
	env string
}

func initAPI(services apid.Services) {
	log.Debug("initialized API's exposed by apidAnalytics plugin")
	analyticsBasePath = config.GetString(configAnalyticsBasePath)
	services.API().HandleFunc(analyticsBasePath + "/{bundle_scope_uuid}", saveAnalyticsRecord).Methods("POST")
}

func saveAnalyticsRecord(w http.ResponseWriter, r *http.Request) {

	db, _ := data.DB()
	if db == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("Still initializing service"))
		return
	}

	vars := apid.API().Vars(r)
	scopeuuid := vars["bundle_scope_uuid"]
	tenant, err := getTenantForScope(scopeuuid)
	if err.errorCode != "" {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		switch err.errorCode {
		case "SEARCH_INTERNAL_ERROR":
			w.WriteHeader(http.StatusInternalServerError)
			if err := json.NewEncoder(w).Encode(errResponse{"SEARCH_INTERNAL_ERROR", err.reason}); err != nil {
				panic(err)
			}
		case "UNKNOWN_SCOPE":
			w.WriteHeader(http.StatusBadRequest)
			if err := json.NewEncoder(w).Encode(errResponse{"UNKNOWN_SCOPE", err.reason}); err != nil {
				panic(err)
			}
		}
	} else {
		message := saveToFile(tenant)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(message))
	}
}

func getTenantForScope(scopeuuid string) (tenant, dbError) {
	// TODO: create a cache during init and refresh it on every failure or listen for snapshot update event
	var org, env string
	{
		db, err := apid.Data().DB()
		switch {
		case err != nil:
			reason := err.Error()
			errorCode := "SEARCH_INTERNAL_ERROR"
			return tenant{org, env}, dbError{reason, errorCode}
		}

		error := db.QueryRow("SELECT env, org FROM DATA_SCOPE WHERE id = ?;", scopeuuid).Scan(&env, &org)

		switch {
		case error == sql.ErrNoRows:
			reason := "No tenant found for this scopeuuid: " + scopeuuid
			errorCode := "UNKNOWN_SCOPE"
			return tenant{org, env}, dbError{reason, errorCode}
		case error != nil:
			reason := error.Error()
			errorCode := "SEARCH_INTERNAL_ERROR"
			return tenant{org, env}, dbError{reason, errorCode}
		}
	}
	return tenant{org, env}, dbError{}
}

func saveToFile(tenant tenant) string {
	message := "hey " + tenant.org + "~" + tenant.env
	return message
}

package apidAnalytics

import (
	"database/sql"
	"encoding/json"
	"github.com/30x/apid"
	"net/http"
	"io"
	"io/ioutil"
	"strings"
	"compress/gzip"
)

type developerInfo struct {
	apiProduct	string
	developerApp	string
	developerEmail	string
	developer	string
}

func getTenantForScope(scopeuuid string) (tenant, dbError) {
	// TODO: create a cache during init and refresh it on every failure or listen for snapshot update event
	var org, env string
	{
		db, err := apid.Data().DB()
		switch {
		case err != nil:
			reason := err.Error()
			errorCode := "INTERNAL_SEARCH_ERROR"
			return tenant{org, env}, dbError{errorCode, reason}
		}

		error := db.QueryRow("SELECT env, org FROM DATA_SCOPE WHERE id = ?;", scopeuuid).Scan(&env, &org)

		switch {
		case error == sql.ErrNoRows:
			reason := "No tenant found for this scopeuuid: " + scopeuuid
			errorCode := "UNKNOWN_SCOPE"
			return tenant{org, env}, dbError{errorCode, reason}
		case error != nil:
			reason := error.Error()
			errorCode := "INTERNAL_SEARCH_ERROR"
			return tenant{org, env}, dbError{errorCode, reason}
		}
	}
	return tenant{org, env}, dbError{}
}


func processPayload(tenant tenant, scopeuuid string,  r *http.Request) errResponse {
	var gzipEncoded bool
	if r.Header.Get("Content-Encoding") != "" {
		if !strings.EqualFold(r.Header.Get("Content-Encoding"),"gzip") {
			return errResponse{"UNSUPPORTED_CONTENT_ENCODING", "Only supported content encoding is gzip"}
		}  else {
			gzipEncoded = true
		}
	}

	var reader io.ReadCloser
	var err error
	if gzipEncoded {
		reader, err = gzip.NewReader(r.Body)			// reader for gzip encoded data
		if err != nil {
			return errResponse{"BAD_DATA", "Gzip data cannot be read"}
		}
	} else {
		reader = r.Body
	}

	body, _ := ioutil.ReadAll(reader)
	errMessage := validateAndEnrich(tenant, scopeuuid,  body)
	if errMessage.ErrorCode != "" {
		return errMessage
	}
	return errResponse{}
}

func validateAndEnrich(tenant tenant, scopeuuid string, body []byte) errResponse {
	var raw map[string]interface{}
	json.Unmarshal(body, &raw)
	if records := raw["records"]; records != nil {
		for _, eachRecord := range records.([]interface{}) {
			recordMap := eachRecord.(map[string]interface{})
			valid, err := validate(recordMap)
			if valid {
				enrich(recordMap, scopeuuid, tenant)
				log.Debugf("Raw records : %v ", eachRecord)
			} else {
				return err				// Even if there is one bad record, then reject entire batch
			}
		}
		// TODO: add the batch of records to a channel for consumption
	} else {
		return errResponse{"NO_RECORDS", "No analytics records in the payload"}
	}
	return errResponse{}
}

func validate(recordMap map[string]interface{}) (bool, errResponse) {
	elems := []string{"client_received_start_timestamp"}
	for _, elem := range elems {
		if recordMap[elem] == nil {
			return false, errResponse{"MISSING_FIELD", "Missing field: " + elem}
		}
	}

	crst, exists1 := recordMap["client_received_start_timestamp"]
	cret, exists2 := recordMap["client_received_end_timestamp"]
	if exists1 && exists2 {
		if crst.(int64) > cret.(int64) {
			return false, errResponse{"BAD_DATA", "client_received_start_timestamp > client_received_end_timestamp"}

		}
	}
	// api key is required to find other info
	_, exists3 := recordMap["client_id"]
	if !exists3 {
		return false, errResponse{"BAD_DATA", "client_id cannot be null"}
	}
	return true, errResponse{}
}

func enrich(recordMap map[string]interface{}, scopeuuid string, tenant tenant) {
	recordMap["organization"] = tenant.org
	recordMap["environment"] = tenant.env
	apiKey := recordMap["client_id"].(string)
	devInfo := getDeveloperInfo(scopeuuid, apiKey)
	recordMap["api_product"] = devInfo.apiProduct
	recordMap["developer_app"] = devInfo.developerApp
	recordMap["developer_email"] = devInfo.developerEmail
	recordMap["developer"] = devInfo.developer
}

// if info not found then dont set it
func getDeveloperInfo(scopeuuid string, apiKey string) developerInfo {
	// TODO: create a cache during init and refresh it on update event
	var apiProduct, developerApp, developerEmail, developer  string
	{
		db, err := apid.Data().DB()
		switch {
		case err != nil:
			return developerInfo{}
		}

		// TODO: query needs to change (wont work, it is just a placeholder)
		error := db.QueryRow("SELECT apiProduct, developerApp, developerEmail, developer FROM DATA_SCOPE WHERE id = ?;", scopeuuid).Scan(&apiProduct, &developerApp, &developerEmail, &developer)

		switch {
		case error == sql.ErrNoRows:
			return developerInfo{}
		case error != nil:
			return developerInfo{}
		}
	}
	return developerInfo{apiProduct, developerApp, developerEmail, developer}
	// For local testing
	//return developerInfo{"test_product", "test_app", "test@test.com", "test"}
}

func writeError(w http.ResponseWriter, status int, code string, reason string) {
	w.WriteHeader(status)
	e := errResponse{
		ErrorCode: code,
		Reason:    reason,
	}
	bytes, err := json.Marshal(e)
	if err != nil {
		log.Errorf("unable to marshal errorResponse: %v", err)
	} else {
		w.Write(bytes)
	}
}

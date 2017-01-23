package apidAnalytics

import (
	"encoding/json"
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
	errMessage := validateEnrichPublish(tenant, scopeuuid,  body)
	if errMessage.ErrorCode != "" {
		return errMessage
	}
	return errResponse{}
}

func validateEnrichPublish(tenant tenant, scopeuuid string, body []byte) errResponse {
	var raw map[string]interface{}
	json.Unmarshal(body, &raw)
	if records := raw["records"]; records != nil {
		for _, eachRecord := range records.([]interface{}) {
			recordMap := eachRecord.(map[string]interface{})
			valid, err := validate(recordMap)
			if valid {
				enrich(recordMap, scopeuuid, tenant)
				// TODO: Remove log
				log.Debugf("Raw records : %v ", eachRecord)
			} else {
				return err				// Even if there is one bad record, then reject entire batch
			}
		}
		publishToChannel(records.([]interface{}))
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
	return true, errResponse{}
}

func enrich(recordMap map[string]interface{}, scopeuuid string, tenant tenant) {
	if recordMap["organization"] == "" {
		recordMap["organization"] = tenant.org

	}
	if recordMap["environment"] == "" {
		recordMap["environment"] = tenant.env
	}
	apiKey, exists := recordMap["client_id"]
	// apiKey doesnt exist then ignore adding developer fields
	if exists {
		apiKey := apiKey.(string)
		devInfo := getDeveloperInfo(tenant.tenantId, apiKey)
		// TODO: Remove log
		log.Debugf("developerInfo = %v",  devInfo)
		if recordMap["api_product"] == "" {
			recordMap["api_product"] = devInfo.apiProduct
		}
		if recordMap["developer_app"] == "" {
			recordMap["developer_app"] = devInfo.developerApp
		}
		if recordMap["developer_email"] == "" {
			recordMap["developer_email"] = devInfo.developerEmail
		}
		if recordMap["developer"] == "" {
			recordMap["developer"] = devInfo.developer
		}
	}
}

func publishToChannel(records []interface{})  {
	// TODO: add the batch of records to a channel for consumption
	return
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

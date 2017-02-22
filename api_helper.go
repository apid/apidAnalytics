package apidAnalytics

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

/*
Implements all the helper methods needed to process the POST /analytics payload
and send it to the internal buffer channel
*/

type developerInfo struct {
	ApiProduct     string
	DeveloperApp   string
	DeveloperEmail string
	Developer      string
}

type axRecords struct {
	Tenant tenant
	// Records is an array of multiple analytics records
	Records []interface{}
}

type tenant struct {
	Org      string
	Env      string
	TenantId string
}

func processPayload(tenant tenant, scopeuuid string, r *http.Request) errResponse {
	var gzipEncoded bool
	if r.Header.Get("Content-Encoding") != "" {
		if !strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
			return errResponse{
				ErrorCode: "UNSUPPORTED_CONTENT_ENCODING",
				Reason:    "Only supported content encoding is gzip"}
		} else {
			gzipEncoded = true
		}
	}

	var reader io.ReadCloser
	var err error
	if gzipEncoded {
		reader, err = gzip.NewReader(r.Body) // reader for gzip encoded data
		if err != nil {
			return errResponse{
				ErrorCode: "BAD_DATA",
				Reason:    "Gzip Encoded data cannot be read"}
		}
	} else {
		reader = r.Body
	}

	errMessage := validateEnrichPublish(tenant, scopeuuid, reader)
	if errMessage.ErrorCode != "" {
		return errMessage
	}
	return errResponse{}
}

func validateEnrichPublish(tenant tenant, scopeuuid string, reader io.Reader) errResponse {
	var raw map[string]interface{}
	decoder := json.NewDecoder(reader) // Decode payload to JSON data
	decoder.UseNumber()

	if err := decoder.Decode(&raw); err != nil {
		return errResponse{ErrorCode: "BAD_DATA",
			Reason: "Not a valid JSON payload"}
	}

	if records := raw["records"]; records != nil {
		// Iterate through each record to validate and enrich it
		for _, eachRecord := range records.([]interface{}) {
			recordMap := eachRecord.(map[string]interface{})
			valid, err := validate(recordMap)
			if valid {
				enrich(recordMap, scopeuuid, tenant)
			} else {
				// Even if there is one bad record, then reject entire batch
				return err
			}
		}
		axRecords := axRecords{
			Tenant:  tenant,
			Records: records.([]interface{})}
		// publish batch of records to channel (blocking call)
		internalBuffer <- axRecords
	} else {
		return errResponse{
			ErrorCode: "NO_RECORDS",
			Reason:    "No analytics records in the payload"}
	}
	return errResponse{}
}

/*
Does basic validation on each analytics message
1. client_received_start_timestamp, client_received_end_timestamp should exist
2. client_received_end_timestamp should be > client_received_start_timestamp and not 0
*/
func validate(recordMap map[string]interface{}) (bool, errResponse) {
	elems := []string{"client_received_start_timestamp", "client_received_end_timestamp"}
	for _, elem := range elems {
		if recordMap[elem] == nil {
			return false, errResponse{
				ErrorCode: "MISSING_FIELD",
				Reason:    "Missing Required field: " + elem}
		}
	}

	crst, exists1 := recordMap["client_received_start_timestamp"]
	cret, exists2 := recordMap["client_received_end_timestamp"]
	if exists1 && exists2 {
		if crst.(json.Number) == json.Number("0") || cret.(json.Number) == json.Number("0") {
			return false, errResponse{
				ErrorCode: "BAD_DATA",
				Reason: "client_received_start_timestamp or " +
					"> client_received_end_timestamp cannot be 0"}
		} else if crst.(json.Number) > cret.(json.Number) {
			return false, errResponse{
				ErrorCode: "BAD_DATA",
				Reason: "client_received_start_timestamp " +
					"> client_received_end_timestamp"}
		}
	}
	return true, errResponse{}
}

/*
Enrich each record by adding org and env fields
It also finds add developer related information based on the apiKey
*/
func enrich(recordMap map[string]interface{}, scopeuuid string, tenant tenant) {
	org, orgExists := recordMap["organization"]
	if !orgExists || org.(string) == "" {
		recordMap["organization"] = tenant.Org
	}

	env, envExists := recordMap["environment"]
	if !envExists || env.(string) == "" {
		recordMap["environment"] = tenant.Env
	}

	apiKey, exists := recordMap["client_id"]
	// apiKey doesnt exist then ignore adding developer fields
	if exists {
		apiKey := apiKey.(string)
		if apiKey != "" {
			devInfo := getDeveloperInfo(tenant.TenantId, apiKey)
			_, exists := recordMap["api_product"]
			if !exists {
				recordMap["api_product"] = devInfo.ApiProduct
			}
			_, exists = recordMap["developer_app"]
			if !exists {
				recordMap["developer_app"] = devInfo.DeveloperApp
			}
			_, exists = recordMap["developer_email"]
			if !exists {
				recordMap["developer_email"] = devInfo.DeveloperEmail
			}
			_, exists = recordMap["developer"]
			if !exists {
				recordMap["developer"] = devInfo.Developer
			}
		}
	}
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

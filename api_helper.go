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

func getJsonBody(r *http.Request) (map[string]interface{}, errResponse) {
	var gzipEncoded bool
	if r.Header.Get("Content-Encoding") != "" {
		if !strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
			return nil, errResponse{
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
			return nil, errResponse{
				ErrorCode: "BAD_DATA",
				Reason:    "Gzip Encoded data cannot be read"}
		}
	} else {
		reader = r.Body
	}

	var raw map[string]interface{}
	decoder := json.NewDecoder(reader) // Decode payload to JSON data
	decoder.UseNumber()

	if err := decoder.Decode(&raw); err != nil {
		return nil, errResponse{ErrorCode: "BAD_DATA",
			Reason: "Not a valid JSON payload"}
	}

	return raw, errResponse{}
}

/*
Get tenant from payload based on the 2 required fields - organization and environment
*/
func getTenantFromPayload(raw map[string]interface{}) (tenant, errResponse) {
	elems := []string{"organization", "environment"}
	for _, elem := range elems {
		if raw[elem] == nil || raw[elem].(string) == "" {
			return tenant{}, errResponse{
				ErrorCode: "MISSING_FIELD",
				Reason:    "Missing Required field: " + elem}
		}
	}

	org := raw["organization"].(string)
	env := raw["environment"].(string)
	return tenant{Org: org, Env: env}, errResponse{}
}

func validateEnrichPublish(tenant tenant, raw map[string]interface{}) errResponse {
	if records := raw["records"]; records != nil {
		records, isArray := records.([]interface{})
		if !isArray {
			return errResponse{
				ErrorCode: "BAD_DATA",
				Reason:    "records should be a list of analytics records"}
		}
		if len(records) == 0 {
			return errResponse{
				ErrorCode: "NO_RECORDS",
				Reason:    "No analytics records in the payload"}
		}
		// Iterate through each record to validate and enrich it
		for _, eachRecord := range records {
			recordMap, isMap := eachRecord.(map[string]interface{})
			if !isMap {
				return errResponse{
					ErrorCode: "BAD_DATA",
					Reason:    "Each Analytics record in records should be a json object"}
			}
			valid, err := validate(recordMap)
			if valid {
				enrich(recordMap, tenant)
			} else {
				// Even if there is one bad record, then reject entire batch
				return err
			}
		}
		axRecords := axRecords{
			Tenant:  tenant,
			Records: records}
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
2. client_received_start_timestamp, client_received_end_timestamp should be a number
3. client_received_end_timestamp should be > client_received_start_timestamp and not 0
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
		crst, isNumber1 := crst.(json.Number)
		cret, isNumber2 := cret.(json.Number)
		if !isNumber1 || !isNumber2 {
			return false, errResponse{
				ErrorCode: "BAD_DATA",
				Reason: "client_received_start_timestamp and " +
					"client_received_end_timestamp has to be number"}
		} else if crst == json.Number("0") || cret == json.Number("0") {
			return false, errResponse{
				ErrorCode: "BAD_DATA",
				Reason: "client_received_start_timestamp or " +
					"client_received_end_timestamp cannot be 0"}
		} else if crst > cret {
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
It also finds and adds developer related information based on the apiKey if not already present in the payload
*/
func enrich(recordMap map[string]interface{}, tenant tenant) {
	// Always overwrite organization/environment value with the tenant information provided in the payload
	recordMap["organization"] = tenant.Org
	recordMap["environment"] = tenant.Env

	apiKey, exists := recordMap["client_id"]
	// apiKey doesnt exist then ignore adding developer fields
	if exists && apiKey != nil {
		apiKey, isString := apiKey.(string)
		if isString {
			devInfo := getDeveloperInfo(tenant.TenantId, apiKey)
			ap, exists := recordMap["api_product"]
			if !exists || ap == nil {
				recordMap["api_product"] = devInfo.ApiProduct
			}
			da, exists := recordMap["developer_app"]
			if !exists || da == nil {
				recordMap["developer_app"] = devInfo.DeveloperApp
			}
			de, exists := recordMap["developer_email"]
			if !exists || de == nil {
				recordMap["developer_email"] = devInfo.DeveloperEmail
			}
			d, exists := recordMap["developer"]
			if !exists || d == nil {
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

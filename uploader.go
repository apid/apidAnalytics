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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const timestampLayout = "20060102150405" // same as yyyyMMddHHmmss

var token string

func addHeaders(req *http.Request) {
	token = config.GetString("apigeesync_bearer_token")
	req.Header.Add("Authorization", "Bearer "+token)
}

func uploadDir(dir os.FileInfo) bool {
	// Eg. org~env~20160101224500
	tenant, timestamp := splitDirName(dir.Name())
	//date=2016-01-01/time=22-45
	dateTimePartition := getDateFromDirTimestamp(timestamp)

	completePath := filepath.Join(localAnalyticsStagingDir, dir.Name())
	files, _ := ioutil.ReadDir(completePath)

	status := true
	var error error
	for _, file := range files {
		completeFilePath := filepath.Join(completePath, file.Name())
		relativeFilePath := dateTimePartition + "/" + file.Name()
		status, error = uploadFile(tenant, relativeFilePath, completeFilePath)
		if error != nil {
			log.Errorf("Upload failed due to: %v", error)
			break
		} else {
			os.Remove(completeFilePath)
			log.Debugf("Deleted file '%s' after "+
				"successful upload", file.Name())
		}
	}
	return status
}

func uploadFile(tenant, relativeFilePath, completeFilePath string) (bool, error) {
	signedUrl, err := getSignedUrl(tenant, relativeFilePath)
	if err != nil {
		return false, err
	} else {
		return uploadFileToDatastore(completeFilePath, signedUrl)
	}
}

func getSignedUrl(tenant, relativeFilePath string) (string, error) {
	uapCollectionUrl := config.GetString(uapServerBase) + "/analytics"

	req, err := http.NewRequest("GET", uapCollectionUrl, nil)
	if err != nil {
		return "", err
	}

	q := req.URL.Query()

	// eg. edgexfeb1~test
	q.Add("tenant", tenant)
	// eg. date=2017-01-30/time=16-32/1069_20170130163200.20170130163400_218e3d99-efaf-4a7b-b3f2-5e4b00c023b7_writer_0.txt.gz
	q.Add("relative_file_path", relativeFilePath)
	q.Add("file_content_type", "application/x-gzip")
	q.Add("encrypt", "true")
	req.URL.RawQuery = q.Encode()

	// Add Bearer Token to each request
	addHeaders(req)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode == 200 {
		var body map[string]interface{}
		json.Unmarshal(respBody, &body)
		signedURL := body["url"]
		return signedURL.(string), nil
	} else {
		return "", fmt.Errorf("Error while getting "+
			"signed URL '%v'", resp.Status)
	}
}

func uploadFileToDatastore(completeFilePath, signedUrl string) (bool, error) {
	// open gzip file that needs to be uploaded
	file, err := os.Open(completeFilePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	req, err := http.NewRequest("PUT", signedUrl, file)
	if err != nil {
		return false, fmt.Errorf("Parsing URL failed '%v'", err)
	}

	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Content-Type", "application/x-gzip")
	req.Header.Set("x-amz-server-side-encryption", "AES256")

	fileStats, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("Could not get content length for "+
			"file '%v'", err)
	}
	req.ContentLength = fileStats.Size()

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		return true, nil
	} else {
		return false, fmt.Errorf("Final Datastore (S3/GCS)returned "+
			"Error '%v'", resp.Status)
	}
}

// Extract tenant and timestamp from directory Name
func splitDirName(dirName string) (string, string) {
	s := strings.Split(dirName, "~")
	tenant := s[0] + "~" + s[1]
	timestamp := s[2]
	return tenant, timestamp
}

// files are uploaded to S3 under specific date time partition and that
// key needs to be generated from the plugin
// eg. <...prefix generated by uap collection service...>/date=2016-01-02/time=15-45/filename.txt.gz
func getDateFromDirTimestamp(timestamp string) string {
	dateTime, _ := time.Parse(timestampLayout, timestamp)
	date := dateTime.Format("2006-01-02") // same as YYYY-MM-dd
	time := dateTime.Format("15-04-05")   // same as HH-mm-ss
	dateHourTS := "date=" + date + "/time=" + time
	return dateHourTS
}

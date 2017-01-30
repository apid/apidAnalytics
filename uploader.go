package apidAnalytics

import (
	"os"
	"encoding/json"
	"strings"
	"path/filepath"
	"io/ioutil"
	"net/http"
	"fmt"
	"time"
)

const (
	maxRetries = 3
	retryFailedDirBatchSize = 10
	timestampLayout = "20060102150405"				// same as yyyyMMddHHmmss
)

var token string

var client *http.Client = &http.Client{
		Timeout: time.Duration(60 * time.Second),		// default timeout of 60 seconds while connecting to s3/GCS
          }

func addHeaders(req *http.Request) {
	token = config.GetString("apigeesync_bearer_token")
	req.Header.Add("Authorization", "Bearer " + token)
}

func uploadDir(dir os.FileInfo) bool {
	tenant, timestamp := splitDirName(dir.Name())
	dateTimePartition := getDateFromDirTimestamp(timestamp)

	completePath := filepath.Join(localAnalyticsStagingDir, dir.Name())
	files, _ := ioutil.ReadDir(completePath)

	status := true
	var error error
	for _, file := range files {
		completeFilePath := filepath.Join(completePath, file.Name())
		relativeFilePath := dateTimePartition + "/" + file.Name();
		status, error = uploadFile(tenant,relativeFilePath, completeFilePath)
		if error != nil {
			log.Errorf("Upload failed due to : %s", error.Error())
			break
		} else {
			os.Remove(completeFilePath)
			log.Debugf("Deleted file after successful upload : %s", file.Name())
		}
	}
	return status
}

func uploadFile(tenant, relativeFilePath, completeFilePath string) (bool, error) {
	signedUrl, err := getSignedUrl(tenant, relativeFilePath, completeFilePath)
	if (err != nil) {
		return false, err
	} else {
		return uploadFileToDatastore(completeFilePath, signedUrl)
	}
}

func getSignedUrl(tenant, relativeFilePath, completeFilePath string) (string, error) {
	uapCollectionUrl := config.GetString(uapServerBase) + "/analytics"

	req, err := http.NewRequest("GET", uapCollectionUrl, nil)
	if err != nil {
		return "", err
	}

	q := req.URL.Query()

	// localTesting
	q.Add("repo", "edge")
	q.Add("dataset", "api")

	q.Add("tenant", tenant)
	q.Add("relative_file_path", relativeFilePath)
	q.Add("file_content_type", "application/x-gzip")
	req.URL.RawQuery = q.Encode()

	addHeaders(req)
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, _ := ioutil.ReadAll(resp.Body)
	if(resp.StatusCode == 200) {
		var body map[string]interface{}
		json.Unmarshal(respBody, &body)
		signedURL :=  body["url"]
		return signedURL.(string), nil
	} else {
		return "", fmt.Errorf("Error while getting signed URL: %s",resp.Status)
	}
}

func uploadFileToDatastore(completeFilePath, signedUrl string) (bool, error) {
	// read gzip file that needs to be uploaded
	file, err := os.Open(completeFilePath)
	if err != nil {
		return false, err
	}
	defer file.Close()

	req, err := http.NewRequest("PUT", signedUrl, file)
	if err != nil {
		return false, fmt.Errorf("Parsing URL failed due to %v", err)
	}

	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Content-Type", "application/x-gzip")

	fileStats, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("Could not get content length for file: %v", err)
	}
	req.ContentLength = fileStats.Size()

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if(resp.StatusCode == 200) {
		return true, nil
	} else {
		return false,fmt.Errorf("Final Datastore (S3/GCS) returned Error: %v ", resp.Status)
	}
}

func splitDirName(dirName string) (string, string){
	s := strings.Split(dirName, "~")
	tenant := s[0]+"~"+s[1]
	timestamp := s[2]
	return  tenant, timestamp
}

// files are uploaded to S3 under specific partition and that key needs to be generated from the plugin
// eg. <...prefix generated by uap collection service...>/date=2016-01-02/time=15-45/filename.txt.gz
func getDateFromDirTimestamp(timestamp string) (string){
	dateTime, _ := time.Parse(timestampLayout, timestamp)
	date := dateTime.Format("2006-01-02")			// same as YYYY-MM-dd
	time :=  dateTime.Format("15-04")			// same as HH-mm
	dateHourTS := "date=" + date  + "/time=" + time
	return dateHourTS
}
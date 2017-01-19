package apidAnalytics

import (
	"os"
	"encoding/json"
	"strings"
	"path/filepath"
	"io/ioutil"
	"net/http"
	"errors"
	"compress/gzip"
)

//var token string

func uploadDir(dir os.FileInfo) bool {

	tenant, timestamp := splitDirName(dir.Name())
	dateTimePartition := getDateFromDirTimestamp(timestamp)
	log.Debugf("tenant: %s | timestamp %s", tenant, timestamp)

	completePath := filepath.Join(localAnalyticsStagingDir, dir.Name())
	files, _ := ioutil.ReadDir(completePath)

	var status bool
	var error error
	for _, file := range files {
		completeFilePath := filepath.Join(completePath, file.Name())
		relativeFilePath := dateTimePartition + "/" + file.Name();
		status, error = uploadFile(tenant,relativeFilePath, completeFilePath)
		if error != nil {
			log.Errorf("Upload File failed due to : %s", error.Error())
			break
		} else {
			os.Remove(completeFilePath)
			log.Debugf("deleted file after successful upload : %s", file.Name())
		}
	}
	return status
}

func uploadFile(tenant, relativeFilePath, completeFilePath string) (bool, error) {

	signedUrl, err := getSignedUrl(tenant, relativeFilePath, completeFilePath)
	if (err != nil) {
		return false, err
	} else {
		log.Debugf("signed URL : %s", signedUrl)
		return true, nil
		//return uploadToDatastore(completeFilePath, signedUrl)
	}
}

func getSignedUrl(tenant, relativeFilePath, completeFilePath string) (string, error) {
	client := &http.Client{}
	uapCollectionUrl := config.GetString(uapServerBase) + "/analytics"

	req, err := http.NewRequest("GET", uapCollectionUrl, nil)
	if err != nil {
		return "", err
	}

	q := req.URL.Query()
	q.Add("tenant", tenant)
	q.Add("relativeFilePath", relativeFilePath)
	q.Add("contentType", "application/x-gzip")
	req.URL.RawQuery = q.Encode()

	// TODO: get bearer token and add as header
	//addHeaders(req)
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
		return "", errors.New("Getting signed URL failed due to " + resp.Status)
	}
}
//func addHeaders(req *http.Request) {
//	req.Header.Add("Authorization", "Bearer " + token)
//}

func uploadToDatastore(completeFilePath, signedUrl string) (bool, error) {
	// read gzip file that needs to be uploaded
	f, err := os.Open(completeFilePath)
	if err != nil {
		return false, err
	}
	defer f.Close()
	reader, err := gzip.NewReader(f)
	if err != nil {
		return false, err
	}

	client := &http.Client{}
	req, err := http.NewRequest("PUT", signedUrl, reader)
	if err != nil {
		return false, err
	}

	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Content-Type", "application/x-gzip")

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if(resp.StatusCode == 200) {
		log.Debugf("response: %v", resp)
		return true, nil
	} else {
		return false, errors.New("Failed to upload file to datastore " + resp.Status)
	}
}

func splitDirName(dirName string) (string, string){
	s := strings.Split(dirName, "~")
	tenant := s[0]+"~"+s[1]
	timestamp := s[2]
	return  tenant, timestamp
}

func getDateFromDirTimestamp(timestamp string) (string){
	return  ""
}
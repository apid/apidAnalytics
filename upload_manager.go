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
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

const (
	maxRetries              = 3
	retryFailedDirBatchSize = 10
)

// Each file upload is retried maxRetries times before
// moving it to failed directory
var retriesMap map[string]int

//TODO:  make sure that this instance gets initialized only once
// since we dont want multiple upload manager tickers running
func initUploadManager() {

	retriesMap = make(map[string]int)

	go func() {
		// Periodically check the staging directory to check
		// if any folders are ready to be uploaded to S3
		ticker := time.NewTicker(time.Second *
			config.GetDuration(analyticsUploadInterval))
		log.Debugf("Intialized upload manager to check for staging directory")
		// Ticker will keep running till go routine is running
		// i.e. till application is running
		defer ticker.Stop()

		for range ticker.C {
			files, err := ioutil.ReadDir(localAnalyticsStagingDir)

			if err != nil {
				log.Errorf("Cannot read directory: "+
					"%s", localAnalyticsStagingDir)
			}

			uploadedDirCnt := 0
			for _, file := range files {
				if file.IsDir() {
					status := uploadDir(file)
					handleUploadDirStatus(file, status)
					if status {
						uploadedDirCnt++
						log.Debugf("Successfully uploaded: %s",
							file.Name())
					}
				}
			}
			if uploadedDirCnt > 0 {
				// After a successful upload, retry the
				// folders in failed directory as they might have
				// failed due to intermitent S3/GCS issue
				retryFailedUploads()
			}
		}
	}()
}

func handleUploadDirStatus(dir os.FileInfo, status bool) {
	completePath := filepath.Join(localAnalyticsStagingDir, dir.Name())
	// If upload is successful then delete files
	// and remove bucket from retry map
	if status {
		os.RemoveAll(completePath)
		log.Debugf("deleted directory after "+
			"successful upload: %s", dir.Name())
		// remove key if exists from retry map after a successful upload
		delete(retriesMap, dir.Name())
	} else {
		retriesMap[dir.Name()] = retriesMap[dir.Name()] + 1
		if retriesMap[dir.Name()] >= maxRetries {
			log.Errorf("Max Retires exceeded for folder: %s", completePath)
			failedDirPath := filepath.Join(localAnalyticsFailedDir, dir.Name())
			err := os.Rename(completePath, failedDirPath)
			if err != nil {
				log.Errorf("Cannot move directory '%s'"+
					" from staging to failed folder", dir.Name())
			}
			// remove key from retry map once it reaches allowed max failed attempts
			delete(retriesMap, dir.Name())
		}
	}
}

func retryFailedUploads() {
	failedDirs, err := ioutil.ReadDir(localAnalyticsFailedDir)

	if err != nil {
		log.Errorf("Cannot read directory: %s", localAnalyticsFailedDir)
	}

	cnt := 0
	for _, dir := range failedDirs {
		// We rety failed folder in batches to not overload the upload thread
		if cnt < retryFailedDirBatchSize {
			failedPath := filepath.Join(localAnalyticsFailedDir, dir.Name())
			newStagingPath := filepath.Join(localAnalyticsStagingDir, dir.Name())
			err := os.Rename(failedPath, newStagingPath)
			if err != nil {
				log.Errorf("Cannot move directory '%s'"+
					" from failed to staging folder", dir.Name())
			}
			cnt++
		} else {
			break
		}
	}
}

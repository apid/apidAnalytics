package apidAnalytics

import (
	_ "fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

var (
	retriesMap map[string]int
)

//TODO:  make sure that this instance gets initialized only once since we dont want multiple upload manager tickers running
func initUploadManager() {

	retriesMap = make(map[string]int)

	// TODO: add a way to make sure that this go routine is always running
	go func() {
		ticker := time.NewTicker(time.Millisecond * config.GetDuration(analyticsUploadInterval) * 1000)
		log.Debugf("Intialized upload manager to check for staging directory")
		defer ticker.Stop() // Ticker will keep running till go routine is running i.e. till application is running

		for t := range ticker.C {
			files, err := ioutil.ReadDir(localAnalyticsStagingDir)

			if err != nil {
				log.Errorf("Cannot read directory %s: ", localAnalyticsStagingDir)
			}

			for _, file := range files {
				log.Debugf("t: %s , file: %s", t, file.Name())
				if file.IsDir() {
					handleUploadDirStatus(file, uploadDir(file))
				}
			}
		}
	}()
}

func handleUploadDirStatus(file os.FileInfo, status bool) {
	completePath := filepath.Join(localAnalyticsStagingDir, file.Name())
	if status {
		os.RemoveAll(completePath)
		// remove key if exists from retry map after a successful upload
		delete(retriesMap, file.Name())
	} else {
		retriesMap[file.Name()] = retriesMap[file.Name()] + 1
		if retriesMap[file.Name()] > maxRetries {
			log.Errorf("Max Retires exceeded for folder: %s", completePath)
			failedDirPath := filepath.Join(localAnalyticsFailedDir, file.Name())
			err := os.Rename(completePath, failedDirPath)
			if err != nil {
				log.Errorf("Cannot move directory :%s to failed folder", file.Name())
			}
			// remove key from retry map once it reaches allowed max failed attempts
			delete(retriesMap, file.Name())
		}
	}
}
package apidAnalytics

import (
	"time"
	"io/ioutil"
	"path/filepath"
	"bufio"
	"os"
	"strings"
	"compress/gzip"
)

const crashRecoveryDelay = 30  // seconds
const recovertTSLayout = "20060102150405.000" // same as "yyyyMMddHHmmss.SSS" format
const fileExtension = ".txt.gz";

const recoveredTS  = "~recoveredTS~"


func initCrashRecovery() {
	if crashRecoveryNeeded() {
		timer := time.After(time.Second * crashRecoveryDelay)
		go func() {
			<- timer
			performRecovery()
		}()
	}
}

func crashRecoveryNeeded() (bool) {
	recoveredDirRecoveryNeeded := recoverFolderInRecoveredDir()
	tmpDirRecoveryNeeded :=  recoverFoldersInTmpDir()
	needed := tmpDirRecoveryNeeded || recoveredDirRecoveryNeeded
	if needed {
		log.Infof("Crash Recovery is needed and will be attempted in %d seconds", crashRecoveryDelay)
	}
	return needed
}

func recoverFoldersInTmpDir() bool {
	tmpRecoveryNeeded := false
	dirs,_ := ioutil.ReadDir(localAnalyticsTempDir)
	recoveryTS := getRecoveryTS()
	for _, dir := range dirs {
		tmpRecoveryNeeded = true
		log.Debugf("Moving directory %s from tmp to recovered ", dir.Name())
		tmpCompletePath := filepath.Join(localAnalyticsTempDir, dir.Name())

		newDirName :=  dir.Name() + recoveredTS + recoveryTS;
		recoveredCompletePath := filepath.Join(localAnalyticsRecoveredDir,newDirName)
		err := os.Rename(tmpCompletePath, recoveredCompletePath)
		if err != nil {
			log.Errorf("Cannot move directory :%s to recovered folder", dir.Name())
		}
	}
	return tmpRecoveryNeeded
}

func getRecoveryTS() string {
	current := time.Now()
	return current.Format(recovertTSLayout)
}

func recoverFolderInRecoveredDir() bool {
	dirs, _ := ioutil.ReadDir(localAnalyticsRecoveredDir)
	if len(dirs) > 0 {
		return true
	}
	return false
}

func performRecovery()  {
	log.Info("Crash recovery is starting...");
	recoveryDirs, _ := ioutil.ReadDir(localAnalyticsRecoveredDir)
	for _, dir := range recoveryDirs {
		recoverDirectory(dir.Name());
	}
	log.Info("Crash recovery complete...");
}

func recoverDirectory(dirName string) {
	log.Infof("performing crash recovery for directory: %s", dirName);
	var bucketRecoveryTS string

	// Parse bucket name to extract recoveryTS and pass it each file to be recovered
	index := strings.Index(dirName, recoveredTS)
	if index != -1 {
		bucketRecoveryTS = "_" + dirName[index+len(recoveredTS):]
	}

	dirBeingRecovered := filepath.Join(localAnalyticsRecoveredDir, dirName)
	files, _ := ioutil.ReadDir(dirBeingRecovered)
	for _, file := range files {
		// recovering each file sequentially for now
		recoverFile(bucketRecoveryTS, dirName, file.Name());
	}

	stagingPath := filepath.Join(localAnalyticsStagingDir, dirName)
	err := os.Rename(dirBeingRecovered, stagingPath)
	if err != nil {
		log.Errorf("Cannot move directory :%s to staging folder", dirName)
	}
}

func recoverFile(bucketRecoveryTS, dirName, fileName string) {
	log.Debugf("performing crash recovery for file: %s ", fileName)
	// add recovery timestamp to the file name
	completeOrigFilePath := filepath.Join(localAnalyticsRecoveredDir, dirName, fileName)
	recoveredExtension := "_recovered" + bucketRecoveryTS + fileExtension
	recoveredFileName := strings.TrimSuffix(fileName, fileExtension) + recoveredExtension
	recoveredFilePath := filepath.Join(localAnalyticsRecoveredDir, dirName, recoveredFileName)
	copyPartialFile(completeOrigFilePath, recoveredFilePath);
	deletePartialFile(completeOrigFilePath);
}

func copyPartialFile(completeOrigFilePath, recoveredFilePath string) {

	// read partial file line by line using buffered gzip reader
	partialFile, err := os.Open(completeOrigFilePath)
	if err != nil {
		log.Errorf("Cannot open file: %s", completeOrigFilePath)
		return
	}
	defer partialFile.Close()

	bufReader := bufio.NewReader(partialFile)
	gzReader, err := gzip.NewReader(bufReader)
	if err != nil {
		log.Errorf("Cannot create reader on gzip file: %s due to %v", completeOrigFilePath, err)
		return
	}
	defer gzReader.Close()

	scanner := bufio.NewScanner(gzReader)

	// Create new file to copy complete records from partial file and upload only a complete file
	recoveredFile, err := os.Create(recoveredFilePath)
	if err != nil {
		log.Errorf("Cannot create recovered file: %s", recoveredFilePath)
		return
	}
	defer recoveredFile.Close()

	bufWriter := bufio.NewWriter(recoveredFile)
	defer bufWriter.Flush()

	gzWriter := gzip.NewWriter(bufWriter)
	defer gzWriter.Close()

	for scanner.Scan() {
		gzWriter.Write(scanner.Bytes())
	}

	if err := scanner.Err(); err != nil {
		log.Errorf("Error while scanning partial file: %v", err)
		return
	}
}

func deletePartialFile(completeOrigFilePath string) {
	err := os.Remove(completeOrigFilePath)
	if err != nil {
		log.Errorf("Cannot delete partial file :%s", completeOrigFilePath)
	}
}
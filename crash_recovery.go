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


const (
	crashRecoveryDelay = 30  // seconds
	recoveryTSLayout = "20060102150405.000" // same as "yyyyMMddHHmmss.SSS" format (Appended to Recovered folder and file)
	recoveredTS  = "~recoveredTS~"	      // Constant to identify recovered files
)

func initCrashRecovery() {
	if crashRecoveryNeeded() {
		timer := time.After(time.Second * crashRecoveryDelay)
		// Actual recovery of files is attempted asynchronously after a delay to not block the apid plugin from starting up
		go func() {
			<- timer
			performRecovery()
		}()
	}
}

// Crash recovery is needed if there are any folders in tmp or recovered directory
func crashRecoveryNeeded() (bool) {
	recoveredDirRecoveryNeeded := recoverFolderInRecoveredDir()
	tmpDirRecoveryNeeded :=  recoverFoldersInTmpDir()
	needed := tmpDirRecoveryNeeded || recoveredDirRecoveryNeeded
	if needed {
		log.Infof("Crash Recovery is needed and will be attempted in %d seconds", crashRecoveryDelay)
	}
	return needed
}

// If Apid is shutdown or crashes while a file is still open in tmp folder, then the file has partial data.
// This partial data can be recoverd.
func recoverFoldersInTmpDir() bool {
	tmpRecoveryNeeded := false
	dirs,_ := ioutil.ReadDir(localAnalyticsTempDir)
	recoveryTS := getRecoveryTS()
	for _, dir := range dirs {
		tmpRecoveryNeeded = true
		log.Debugf("Moving directory '%s' from tmp to recovered folder", dir.Name())
		tmpCompletePath := filepath.Join(localAnalyticsTempDir, dir.Name())
		newDirName :=  dir.Name() + recoveredTS + recoveryTS;			// Eg. org~env~20160101222400~recoveredTS~20160101222612.123
		recoveredCompletePath := filepath.Join(localAnalyticsRecoveredDir,newDirName)
		err := os.Rename(tmpCompletePath, recoveredCompletePath)
		if err != nil {
			log.Errorf("Cannot move directory '%s' from tmp to recovered folder", dir.Name())
		}
	}
	return tmpRecoveryNeeded
}

// Get Timestamp for when the recovery is being attempted on the folder.
func getRecoveryTS() string {
	current := time.Now()
	return current.Format(recoveryTSLayout)
}

// If APID is restarted twice immediately such that files have been moved to recovered folder but actual recovery has'nt started or is partially done
// Then the files will just stay in the recovered dir and need to be recovered again.
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
	// Eg. org~env~20160101222400~recoveredTS~20160101222612.123 -> bucketRecoveryTS = _20160101222612.123
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
		log.Errorf("Cannot move directory '%s' from recovered to staging folder", dirName)
	}
}

func recoverFile(bucketRecoveryTS, dirName, fileName string) {
	log.Debugf("performing crash recovery for file: %s ", fileName)
	// add recovery timestamp to the file name
	completeOrigFilePath := filepath.Join(localAnalyticsRecoveredDir, dirName, fileName)

	recoveredExtension := "_recovered" + bucketRecoveryTS + fileExtension
	recoveredFileName := strings.TrimSuffix(fileName, fileExtension) + recoveredExtension
	// eg. 5be1_20170130155400.20170130155600_218e3d99-efaf-4a7b-b3f2-5e4b00c023b7_writer_0_recovered_20170130155452.616.txt
	recoveredFilePath := filepath.Join(localAnalyticsRecoveredDir, dirName, recoveredFileName)

	// Copy complete records to new file and delete original partial file
	copyPartialFile(completeOrigFilePath, recoveredFilePath);
	deletePartialFile(completeOrigFilePath);
}

// The file is read line by line and all complete records are extracted and copied to a new file which is closed as a correct gzip file.
func copyPartialFile(completeOrigFilePath, recoveredFilePath string) {
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
	recoveredFile, err := os.OpenFile(recoveredFilePath, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		log.Errorf("Cannot create recovered file: %s", recoveredFilePath)
		return
	}
	defer recoveredFile.Close()

	gzWriter := gzip.NewWriter(recoveredFile)
	defer gzWriter.Close()

	bufWriter := bufio.NewWriter(gzWriter)
	defer bufWriter.Flush()

	for scanner.Scan() {
		bufWriter.Write(scanner.Bytes())
		bufWriter.WriteString("\n")
	}
	if err := scanner.Err(); err != nil {
		log.Warnf("Error while scanning partial file: %v", err)
		return
	}
}

func deletePartialFile(completeOrigFilePath string) {
	err := os.Remove(completeOrigFilePath)
	if err != nil {
		log.Errorf("Cannot delete partial file: %s", completeOrigFilePath)
	}
}
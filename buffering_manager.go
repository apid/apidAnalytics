package apidAnalytics

import (
	"time"
	"os"
	"bufio"
	"compress/gzip"
	"path/filepath"
	"fmt"
	"crypto/rand"
	"encoding/json"
)

var internalBuffer chan axRecords
var closeBucketEvent chan bucket
var bucketMap map[int64]bucket

type bucket struct {
	DirName string
	// We need file handle, writter pointer to close the file
	FileWriter fileWriter
}

// This struct will store open file handle and writer to close the file
type fileWriter struct {
	file  *os.File
	gw *gzip.Writer
	bw *bufio.Writer
}

func initBufferingManager() {
	internalBuffer = make(chan axRecords, config.GetInt(analyticsBufferChannelSize))
	closeBucketEvent = make(chan  bucket)
	bucketMap = make(map[int64]bucket)

	// Keep polling the internal buffer for new messages
	go func() {
		for  {
			records := <-internalBuffer
			err := save(records)
			if err != nil {
				log.Errorf("Could not save %d messages to file. %v", len(records.Records), err)
			}
		}
	}()

	// Keep polling the closeEvent channel to see if bucket is ready to be closed
	go func() {
		for  {
			bucket := <- closeBucketEvent
			log.Debugf("Closing bucket %s", bucket.DirName)

			// close open file
			closeGzipFile(bucket.FileWriter)

			dirToBeClosed := filepath.Join(localAnalyticsTempDir, bucket.DirName)
			stagingPath := filepath.Join(localAnalyticsStagingDir, bucket.DirName)
			err := os.Rename(dirToBeClosed, stagingPath)
			if err != nil {
				log.Errorf("Cannot move directory :%s to staging folder", bucket.DirName)
			}
		}
	}()
}

func save(records axRecords) (error) {
	bucket, err := getBucketForTimestamp(time.Now(), records.Tenant)
	if (err != nil ) {
		return err
	}
	writeGzipFile(bucket.FileWriter, records.Records)
	return nil
}


func getBucketForTimestamp(now time.Time, tenant tenant) (bucket, error) {
	// first based on current timestamp, determine the timestamp bucket
	ts :=  now.Unix() / int64(config.GetInt(analyticsCollectionInterval)) * int64(config.GetInt(analyticsCollectionInterval))
	_, exists := bucketMap[ts]
	if exists {
		return bucketMap[ts], nil
	} else {
		timestamp := time.Unix(ts, 0).Format(timestampLayout)

		endTime := time.Unix(ts + int64(config.GetInt(analyticsCollectionInterval)), 0)
		endtimestamp := endTime.Format(timestampLayout)

		dirName := tenant.Org + "~" + tenant.Env + "~" + timestamp
		newPath := filepath.Join(localAnalyticsTempDir, dirName)
		// create dir
		err := os.Mkdir(newPath, os.ModePerm)
		if err != nil {
			return bucket{}, fmt.Errorf("Cannot create directory : %s to buffer messages due to %v:", dirName, err)
		}

		// create file for writing
		fileName := getRandomHex() + "_" + timestamp + "." + endtimestamp + "_" + config.GetString("apigeesync_apid_instance_id") + "_writer_0.txt.gz"
		completeFilePath := filepath.Join(newPath, fileName)
		fw, err := createGzipFile(completeFilePath)
		if err != nil {
			return bucket{}, err
		}

		newBucket := bucket{DirName: dirName, FileWriter: fw}
		bucketMap[ts] = newBucket

		//Send event to close directory after endTime
		timer := time.After(endTime.Sub(time.Now()) + time.Second * 5)
		go func() {
			<- timer
			closeBucketEvent <- newBucket
		}()
		return newBucket, nil
	}
}

//TODO: implement 4 digit hext method
func getRandomHex() string {
	buff := make([]byte, 2)
	rand.Read(buff)
	return fmt.Sprintf("%x", buff)
}

func createGzipFile(s string) (fileWriter, error) {
	file, err := os.OpenFile(s, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return fileWriter{},fmt.Errorf("Cannot create file : %s to buffer messages due to: %v", s, err)
	}
	gw := gzip.NewWriter(file)
	bw := bufio.NewWriter(gw)
	return fileWriter{file, gw, bw}, nil
}

func writeGzipFile(fw fileWriter, records []interface{}) {
	for _, eachRecord := range records {
		s, _ := json.Marshal(eachRecord)
		_, err := (fw.bw).WriteString(string(s))
		if err != nil {
			log.Errorf("Write to file failed due to: %v", err)
		}
		(fw.bw).WriteString("\n")
	}
	fw.bw.Flush()
}

func closeGzipFile(fw fileWriter) {
	fw.bw.Flush()
	// Close the gzip first.
	fw.gw.Close()
	fw.file.Close()
}


package apidAnalytics

import (
	"bufio"
	"compress/gzip"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const fileExtension = ".txt.gz"

// Channel where analytics records are buffered before being dumped to a file as write to file should not performed in the Http Thread
var internalBuffer chan axRecords

// Channel where close bucket event is published i.e. when a bucket is ready to be closed based on collection interval
var closeBucketEvent chan bucket

// Map from timestampt to bucket
var bucketMap map[int64]bucket

type bucket struct {
	DirName string
	// We need file handle and writer to close the file
	FileWriter fileWriter
}

// This struct will store open file handle and writer to close the file
type fileWriter struct {
	file *os.File
	gw   *gzip.Writer
	bw   *bufio.Writer
}

func initBufferingManager() {
	internalBuffer = make(chan axRecords, config.GetInt(analyticsBufferChannelSize))
	closeBucketEvent = make(chan bucket)
	bucketMap = make(map[int64]bucket)

	// Keep polling the internal buffer for new messages
	go func() {
		for {
			records := <-internalBuffer
			err := save(records)
			if err != nil {
				log.Errorf("Could not save %d messages to file due to: %v", len(records.Records), err)
			}
		}
	}()

	// Keep polling the closeEvent channel to see if bucket is ready to be closed
	go func() {
		for {
			bucket := <-closeBucketEvent
			log.Debugf("Close Event received for bucket: %s", bucket.DirName)

			// close open file
			closeGzipFile(bucket.FileWriter)

			dirToBeClosed := filepath.Join(localAnalyticsTempDir, bucket.DirName)
			stagingPath := filepath.Join(localAnalyticsStagingDir, bucket.DirName)
			// close files in tmp folder and move directory to staging to indicate its ready for upload
			err := os.Rename(dirToBeClosed, stagingPath)
			if err != nil {
				log.Errorf("Cannot move directory '%s' from tmp to staging folder", bucket.DirName)
			}
		}
	}()
}

// Save records to correct file based on what timestamp data is being collected for
func save(records axRecords) error {
	bucket, err := getBucketForTimestamp(time.Now(), records.Tenant)
	if err != nil {
		return err
	}
	writeGzipFile(bucket.FileWriter, records.Records)
	return nil
}

func getBucketForTimestamp(now time.Time, tenant tenant) (bucket, error) {
	// first based on current timestamp and collection interval, determine the timestamp of the bucket
	ts := now.Unix() / int64(config.GetInt(analyticsCollectionInterval)) * int64(config.GetInt(analyticsCollectionInterval))
	_, exists := bucketMap[ts]
	if exists {
		return bucketMap[ts], nil
	} else {
		timestamp := time.Unix(ts, 0).Format(timestampLayout)

		// endtimestamp of bucket = starttimestamp + collectionInterval
		endTime := time.Unix(ts+int64(config.GetInt(analyticsCollectionInterval)), 0)
		endtimestamp := endTime.Format(timestampLayout)

		dirName := tenant.Org + "~" + tenant.Env + "~" + timestamp
		newPath := filepath.Join(localAnalyticsTempDir, dirName)
		// create dir
		err := os.Mkdir(newPath, os.ModePerm)
		if err != nil {
			return bucket{}, fmt.Errorf("Cannot create directory '%s' to buffer messages '%v'", dirName, err)
		}

		// create file for writing
		// Format: <4DigitRandomHex>_<TSStart>.<TSEnd>_<APIDINSTANCEUUID>_writer_0.txt.gz
		fileName := getRandomHex() + "_" + timestamp + "." + endtimestamp + "_" + config.GetString("apigeesync_apid_instance_id") + "_writer_0" + fileExtension
		completeFilePath := filepath.Join(newPath, fileName)
		fw, err := createGzipFile(completeFilePath)
		if err != nil {
			return bucket{}, err
		}

		newBucket := bucket{DirName: dirName, FileWriter: fw}
		bucketMap[ts] = newBucket

		//Send event to close directory after endTime + 5 seconds to make sure all buffers are flushed to file
		timer := time.After(endTime.Sub(time.Now()) + time.Second*5)
		go func() {
			<-timer
			closeBucketEvent <- newBucket
		}()
		return newBucket, nil
	}
}

// 4 digit Hex is prefixed to each filename to improve how s3 partitions the files being uploaded
func getRandomHex() string {
	buff := make([]byte, 2)
	rand.Read(buff)
	return fmt.Sprintf("%x", buff)
}

func createGzipFile(s string) (fileWriter, error) {
	file, err := os.OpenFile(s, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return fileWriter{}, fmt.Errorf("Cannot create file '%s' to buffer messages '%v'", s, err)
	}
	gw := gzip.NewWriter(file)
	bw := bufio.NewWriter(gw)
	return fileWriter{file, gw, bw}, nil
}

func writeGzipFile(fw fileWriter, records []interface{}) {
	// write each record as a new line to the bufferedWriter
	for _, eachRecord := range records {
		s, _ := json.Marshal(eachRecord)
		_, err := (fw.bw).WriteString(string(s))
		if err != nil {
			log.Errorf("Write to file failed '%v'", err)
		}
		(fw.bw).WriteString("\n")
	}
	// Flush entire batch of records to file vs each message
	fw.bw.Flush()
	fw.gw.Flush()
}

func closeGzipFile(fw fileWriter) {
	fw.bw.Flush()
	fw.gw.Close()
	fw.file.Close()
}

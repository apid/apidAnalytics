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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
	"path/filepath"
	"time"
)

var _ = Describe("test getBucketForTimestamp()", func() {
	It("should return new bucket or existing bucket if created previously", func() {
		t := time.Date(2017, 1, 20, 10, 24, 5, 0, time.UTC)
		tenant := tenant{Org: "testorg", Env: "testenv", TenantId: "tenantid"}

		bucket, err := getBucketForTimestamp(t, tenant)
		Expect(err).ShouldNot(HaveOccurred())

		Expect(bucket.DirName).To(Equal("testorg~testenv~20170120102400"))
		Expect(bucket.FileWriter).ToNot(BeNil())

		fw := bucket.FileWriter
		Expect(fw.file.Name()).To(ContainSubstring("20170120102400.20170120102600"))

		// Should return existing bucket if same interval timestamp is passed
		t2 := time.Date(2017, 1, 20, 10, 25, 5, 0, time.UTC)
		bucket, err = getBucketForTimestamp(t2, tenant)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(bucket.DirName).To(Equal("testorg~testenv~20170120102400"))
	})
})

var _ = Describe("test getRandomHex()", func() {
	It("should return a 4 digit hex", func() {
		r1 := getRandomHex()
		Expect(len(r1)).To(Equal(4))
	})

	It("should return differe 4 digit hex each time", func() {
		r1 := getRandomHex()
		r2 := getRandomHex()

		Expect(r1).NotTo(Equal(r2))
	})
})

var _ = Describe("test createWriteAndCloseFile()", func() {
	Context("Cannot create file", func() {
		It("should return error", func() {
			fileName := "testFile" + fileExtension
			completeFilePath := filepath.Join(localAnalyticsTempDir, "fakedir", fileName)

			_, err := createGzipFile(completeFilePath)
			Expect(err).To((HaveOccurred()))
		})

	})
	Context("Create file, write to it and close file", func() {
		It("should save content to file and read correctly", func() {
			fileName := "testFile" + fileExtension
			completeFilePath := filepath.Join(localAnalyticsTempDir, fileName)

			fw, err := createGzipFile(completeFilePath)
			Expect(err).ToNot((HaveOccurred()))

			var records = []byte(`{
					"records":[{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp": 1486406248277,
						"client_received_end_timestamp": 1486406248290
					}]
				}`)

			raw := getRaw(records)

			writeGzipFile(fw, raw["records"].([]interface{}))
			closeGzipFile(fw)

			// Verify file was written to properly
			f, err := os.Open(completeFilePath)
			defer f.Close()
			gzReader, err := gzip.NewReader(f)
			defer gzReader.Close()
			Expect(err).ToNot((HaveOccurred()))

			var record map[string]interface{}
			decoder := json.NewDecoder(gzReader) // Decode payload to JSON data
			decoder.UseNumber()
			err = decoder.Decode(&record)
			Expect(err).ToNot((HaveOccurred()))

			Expect(record["client_id"]).To(Equal("testapikey"))
			Expect(record["response_status_code"]).To(Equal(json.Number("200")))
			Expect(record["client_received_start_timestamp"]).To(Equal(json.Number("1486406248277")))
			Expect(record["client_received_end_timestamp"]).To(Equal(json.Number("1486406248290")))

			err = os.Remove(completeFilePath)
			Expect(err).ToNot((HaveOccurred()))
		})
	})
})

var _ = Describe("test closeBucketChannel()", func() {
	Context("send close bucket event on channel", func() {
		It("close file and move to staging dir", func() {
			dirName := "testorg~testenv~20160101230000"
			dirPath := filepath.Join(localAnalyticsTempDir, dirName)

			err := os.Mkdir(dirPath, os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())

			fileName := "testFile" + fileExtension
			completeFilePath := filepath.Join(dirPath, fileName)

			fw, e := createGzipFile(completeFilePath)
			Expect(e).ShouldNot(HaveOccurred())

			bucket := bucket{keyTS: 112312, DirName: dirName, FileWriter: fw}
			closeBucketEvent <- bucket

			// wait for it to close dir and move to staging
			time.Sleep(time.Second * 2)

			expectedDirPath := filepath.Join(localAnalyticsStagingDir, dirName)
			Expect(expectedDirPath).To(BeADirectory())

			expectedfilePath := filepath.Join(localAnalyticsStagingDir, dirName, fileName)
			Expect(expectedfilePath).To(BeAnExistingFile())
		})
	})
})

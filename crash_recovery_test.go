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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

var _ = Describe("test crashRecoveryNeeded(), ", func() {
	Context("directories in recovered folder", func() {
		It("should return true", func() {
			dirName := "t~e~20160108536000~recoveredTS~20160101222612.123"
			dirPath := filepath.Join(localAnalyticsRecoveredDir, dirName)

			err := os.Mkdir(dirPath, os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())

			needed := crashRecoveryNeeded()
			Expect(needed).To(BeTrue())

			err = os.Remove(dirPath)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
	Context("directories in tmp folder", func() {
		It("should return true", func() {
			d := "t~e~20160112630000"
			dirPath := filepath.Join(localAnalyticsTempDir, d)
			fp := filepath.Join(dirPath, "fakefile.txt.gz")
			err := os.Mkdir(dirPath, os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = os.Create(fp)
			Expect(err).ShouldNot(HaveOccurred())

			needed := crashRecoveryNeeded()
			Expect(needed).To(BeTrue())

			// moves file to recovered dir
			dirs, _ := ioutil.ReadDir(localAnalyticsRecoveredDir)
			for _, dir := range dirs {
				if strings.Contains(dir.Name(), d) {
					Expect(dir.Name()).To(
						ContainSubstring(d + recoveredTS))
					err = os.RemoveAll(localAnalyticsRecoveredDir +
						"/" + dir.Name())
					Expect(err).ShouldNot(HaveOccurred())
				}
			}
		})
	})
})

var _ = Describe("test performRecovery(), ", func() {
	It("should move all recovered directories to staging", func() {
		dirName := "t~e~20160101545000~recoveredTS~20160101222612.123"
		dirPath := filepath.Join(localAnalyticsRecoveredDir, dirName)
		err := os.Mkdir(dirPath, os.ModePerm)
		Expect(err).ShouldNot(HaveOccurred())

		performRecovery()
		newPath := filepath.Join(localAnalyticsStagingDir, dirName)
		Expect(newPath).To(BeADirectory())

		err = os.Remove(newPath)
		Expect(err).ShouldNot(HaveOccurred())
	})
})

var _ = Describe("test recoverDirectory(), ", func() {
	It("should recover file and move folder to staging", func() {
		dirName := "t~e~20160101535000~recoveredTS~20160101222612.123"
		dirPath := filepath.Join(localAnalyticsRecoveredDir, dirName)
		fp := filepath.Join(dirPath, "fakefile.txt.gz")
		err := os.Mkdir(dirPath, os.ModePerm)
		Expect(err).ShouldNot(HaveOccurred())

		recoveredFile, err := os.OpenFile(fp,
			os.O_WRONLY|os.O_CREATE, os.ModePerm)
		Expect(err).ShouldNot(HaveOccurred())

		gw := gzip.NewWriter(recoveredFile)

		// write some content to file
		var records = []byte(`{
					"response_status_code": 200,
					"client_id":"testapikey",
					"client_received_start_timestamp": 1486406248277,
					"client_received_end_timestamp": 1486406248290
				}`)
		gw.Write(records)
		gw.Close()
		recoveredFile.Close()

		recoverDirectory(dirName)

		stagingPath := filepath.Join(localAnalyticsStagingDir, dirName)
		Expect(dirPath).ToNot(BeADirectory())
		Expect(stagingPath).To(BeADirectory())

		err = os.RemoveAll(stagingPath)
		Expect(err).ShouldNot(HaveOccurred())
	})
})

var _ = Describe("test recoverFile(), ", func() {
	It("should create a recovered file and delete parital file", func() {
		dirName := "t~e~20160101530000~recoveredTS~20160101222612.123"
		dirPath := filepath.Join(localAnalyticsRecoveredDir, dirName)
		fp := filepath.Join(dirPath, "fakefile.txt.gz")
		err := os.Mkdir(dirPath, os.ModePerm)
		Expect(err).ShouldNot(HaveOccurred())

		recoveredFile, err := os.OpenFile(fp,
			os.O_WRONLY|os.O_CREATE, os.ModePerm)
		Expect(err).ShouldNot(HaveOccurred())

		gw := gzip.NewWriter(recoveredFile)

		// write some content to file
		var records = []byte(`{
					"response_status_code": 200,
					"client_id":"testapikey",
					"client_received_start_timestamp": 1486406248277,
					"client_received_end_timestamp": 1486406248290
				}`)
		gw.Write(records)
		gw.Close()
		recoveredFile.Close()

		recoverFile("_20160101222612.123", dirName, "fakefile.txt.gz")

		recoveredFileName := "fakefile_recovered_20160101222612.123.txt.gz"
		recoveredFilePath := filepath.Join(dirPath, recoveredFileName)
		Expect(recoveredFilePath).To(BeAnExistingFile())
		Expect(fp).ToNot(BeAnExistingFile())

		// Verify file was written to properly
		f, err := os.Open(recoveredFilePath)
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
		Expect(record["client_received_start_timestamp"]).
			To(Equal(json.Number("1486406248277")))
		Expect(record["client_received_end_timestamp"]).
			To(Equal(json.Number("1486406248290")))

		err = os.RemoveAll(dirPath)
		Expect(err).ShouldNot(HaveOccurred())
	})
})

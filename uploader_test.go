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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
	"path/filepath"
)

var _ = Describe("test uploadFile()", func() {
	It("should return status based on tenant", func() {
		fakeDir := filepath.Join(localAnalyticsStagingDir, "testorg~testenv~20060102150405")
		fp := filepath.Join(fakeDir, "fakefile.txt.gz")
		os.Mkdir(fakeDir, os.ModePerm)
		os.Create(fp)

		By("valid tenant")
		tenant := "testorg~testenv"
		relativeFilePath := "/date=2006-01-02/time=15-04-05/fakefile.txt.gz"

		status, err := uploadFile(tenant, relativeFilePath, fp)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(status).To(BeTrue())

		By("invalid tenant")
		tenant = "o~e"
		relativeFilePath = "/date=2006-01-02/time=15-04-05/fakefile.txt.gz"

		status, err = uploadFile(tenant, relativeFilePath, fp)
		Expect(err).Should(HaveOccurred())
		Expect(status).To(BeFalse())
	})
})

var _ = Describe("test uploadDir()", func() {
	Context("valid tenant", func() {
		It("should return true and delete the file", func() {
			fakeDir := filepath.Join(localAnalyticsStagingDir, "testorg~testenv~20060102150605")
			fp := filepath.Join(fakeDir, "fakefile.txt.gz")
			os.Mkdir(fakeDir, os.ModePerm)
			os.Create(fp)

			dir, _ := os.Stat(fakeDir)

			status := uploadDir(dir)
			Expect(status).To(BeTrue())
			Expect(fp).ToNot(BeAnExistingFile())
		})
	})
	Context("invalid tenant", func() {
		It("should return false and file should not be deleted", func() {
			fakeDir := filepath.Join(localAnalyticsStagingDir, "o~e~20060102150605")
			fp := filepath.Join(fakeDir, "fakefile.txt.gz")
			os.Mkdir(fakeDir, os.ModePerm)
			os.Create(fp)

			dir, _ := os.Stat(fakeDir)

			status := uploadDir(dir)
			Expect(status).To(BeFalse())
			Expect(fp).To(BeAnExistingFile())
		})
	})
})

var _ = Describe("test getSignedUrl()", func() {
	Context("invalid tenant", func() {
		It("should return error", func() {
			tenant := "org~env"
			relativeFilePath := "/date=2016-01-01/time=22-45-05/a.txt.gz"

			_, err := getSignedUrl(tenant, relativeFilePath)
			Expect(err).Should(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("404 Not Found"))
		})
	})
	Context("valid tenant", func() {
		It("should return signed url", func() {
			tenant := "testorg~testenv"
			relativeFilePath := "/date=2016-01-01/time=22-45-05/a.txt.gz"

			url, err := getSignedUrl(tenant, relativeFilePath)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(url).ShouldNot(Equal(""))
		})
	})
})

var _ = Describe("test uploadFileToDatastore()", func() {
	It("should return status based on response from mocked datastore", func() {
		fakeDir := filepath.Join(localAnalyticsStagingDir, "d1~e1~20060102150405")
		fp := filepath.Join(fakeDir, "fakefile.txt")
		os.Mkdir(fakeDir, os.ModePerm)
		os.Create(fp)

		By("trying to upload not existng file")
		signedUrl := testServer.URL + "/upload?expected_status=ok"
		status, err := uploadFileToDatastore("nofile", signedUrl)
		Expect(err).Should(HaveOccurred())
		Expect(status).To(BeFalse())

		By("successful file upload")
		signedUrl = testServer.URL + "/upload?expected_status=ok"
		status, err = uploadFileToDatastore(fp, signedUrl)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(status).To(BeTrue())

		By("trying to upload file after signed url has expired")
		signedUrl = testServer.URL + "/upload?expected_status=forbidden"
		status, err = uploadFileToDatastore(fp, signedUrl)
		Expect(status).To(BeFalse())
		Expect(err).Should(HaveOccurred())
		Expect(err.Error()).Should(ContainSubstring("403 Forbidden"))

		By("internal server error from datastore")
		signedUrl = testServer.URL + "/upload?expected_status=serverError"
		status, err = uploadFileToDatastore(fp, signedUrl)
		Expect(status).To(BeFalse())
		Expect(err).Should(HaveOccurred())
		Expect(err.Error()).Should(
			ContainSubstring("500 Internal Server Error"))

	})
})

var _ = Describe("test splitDirName()", func() {
	It("should return tenant and timestamp", func() {
		dirname := "o1~e1~20060102150405"
		tenant, timestamp := splitDirName(dirname)
		Expect(tenant).To(Equal("o1~e1"))
		Expect(timestamp).To(Equal("20060102150405"))
	})
})

var _ = Describe("test getDateFromDirTimestamp()", func() {
	It("should return date/time formatted timestamp", func() {
		timestamp := "20060102150405"
		dateHourTS := getDateFromDirTimestamp(timestamp)
		Expect(dateHourTS).To(Equal("date=2006-01-02/time=15-04-05"))
	})
})

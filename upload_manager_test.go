package apidAnalytics

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
)

var _ = Describe("test handleUploadDirStatus()", func() {
	Context("successful upload", func() {
		It("should delete dir from staging and remove entry from map", func() {
			dirName := "testorg~testenv~20160101530000"
			dirPath := filepath.Join(localAnalyticsStagingDir, dirName)

			err := os.Mkdir(dirPath, os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())

			info, e := os.Stat(dirPath)
			Expect(e).ShouldNot(HaveOccurred())
			handleUploadDirStatus(info, true)

			Expect(dirPath).ToNot(BeADirectory())

			_, exists := retriesMap[dirName]
			Expect(exists).To(BeFalse())
		})
	})
	Context("unsuccessful upload", func() {
		It("retry thrice before moving to failed", func() {
			dirName := "testorg~testenv~20160101530000"
			dirPath := filepath.Join(localAnalyticsStagingDir, dirName)

			err := os.Mkdir(dirPath, os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())

			info, e := os.Stat(dirPath)
			Expect(e).ShouldNot(HaveOccurred())

			// Retry thrice
			for i := 1; i < maxRetries; i++ {
				handleUploadDirStatus(info, false)

				Expect(dirPath).To(BeAnExistingFile())

				cnt, exists := retriesMap[dirName]
				Expect(exists).To(BeTrue())
				Expect(cnt).To(Equal(i))
			}

			// after final retry, it should be moved to failed
			handleUploadDirStatus(info, false)

			failedPath := filepath.Join(localAnalyticsFailedDir, dirName)
			Expect(failedPath).To(BeADirectory())

			_, exists := retriesMap[dirName]
			Expect(exists).To(BeFalse())
		})
	})
})

var _ = Describe("test retryFailedUploads()", func() {
	Context("previously failed directories in failed folder", func() {
		It("should be moved to staging directory", func() {
			dirName := "testorg~testenv~20160101830000"
			dirPath := filepath.Join(localAnalyticsFailedDir, dirName)

			err := os.Mkdir(dirPath, os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred())

			retryFailedUploads()

			stagingPath := filepath.Join(localAnalyticsStagingDir, dirName)

			// move from failed to staging directory
			Expect(dirPath).ToNot(BeADirectory())
			Expect(stagingPath).To(BeADirectory())
		})
		It("if multiple folders, then move only configured batch at a time", func() {
			for i := 1; i < (retryFailedDirBatchSize * 2); i++ {
				dirName := "testorg~testenv" + strconv.Itoa(i) + "~2016010183000"
				dirPath := filepath.Join(localAnalyticsFailedDir, dirName)
				err := os.Mkdir(dirPath, os.ModePerm)
				Expect(err).ShouldNot(HaveOccurred())
			}

			// before count failed
			dirs, _ := ioutil.ReadDir(localAnalyticsFailedDir)
			failedDirCntBefore := len(dirs)

			retryFailedUploads()

			// after count failed
			dirs, _ = ioutil.ReadDir(localAnalyticsFailedDir)
			failedDirCntAfter := len(dirs)

			Expect(failedDirCntBefore - failedDirCntAfter).
				To(Equal(retryFailedDirBatchSize))

		})
	})
})

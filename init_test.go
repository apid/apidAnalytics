package apidAnalytics

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"path/filepath"
)

var _ = Describe("test createDirectories()", func() {
	Context("Parent directory exists", func() {
		It("should create sub directory", func() {
			subDir := filepath.Join(config.GetString("data_path"), "subDir")
			directories := []string{subDir}

			err := createDirectories(directories)
			Expect(err).NotTo(HaveOccurred())
		})
	})
	Context("Parent directory does not exists", func() {
		subDir := filepath.Join("/fakepath", "subDir")
		directories := []string{subDir}

		It("sub directory creation should fail", func() {
			err := createDirectories(directories)
			Expect(err).To(HaveOccurred())
		})
	})
})

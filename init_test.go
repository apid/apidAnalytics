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

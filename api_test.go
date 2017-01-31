package apidAnalytics

import (
	"net/http"
	"net/url"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// BeforeSuite setup and AfterSuite cleanup is in apidAnalytics_suite_test.go
var _ = Describe("testing saveAnalyticsRecord() directly", func() {
	Context("valid scopeuuid", func() {
		It("should successfully return", func() {
			uri, err := url.Parse(testServer.URL)
			uri.Path = analyticsBasePath

			v := url.Values{}
			v.Add("bundle_scope_uuid", "testid")

			client := &http.Client{}
			req, err := http.NewRequest("POST", uri.String(), strings.NewReader(v.Encode()))
			res, err := client.Do(req)
			defer res.Body.Close()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res.StatusCode, http.StatusOK)
		})
	})

	Context("invalid scopeuuid", func() {
		It("should return bad request", func() {
			uri, err := url.Parse(testServer.URL)
			uri.Path = analyticsBasePath

			v := url.Values{}
			v.Add("bundle_scope_uuid", "wrongId")

			client := &http.Client{}
			req, err := http.NewRequest("POST", uri.String(), strings.NewReader(v.Encode()))
			res, err := client.Do(req)
			defer res.Body.Close()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res.StatusCode, http.StatusBadRequest)
		})
	})
})

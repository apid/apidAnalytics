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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// BeforeSuite setup and AfterSuite cleanup is in apidAnalytics_suite_test.go
var _ = Describe("POST /analytics/{scopeuuid}", func() {
	Context("invalid content type header", func() {
		It("should return bad request", func() {
			req := getRequestWithScope("test", nil)
			req.Header.Set("Content-Type", "application/x-gzip")

			res, e := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("UNSUPPORTED_CONTENT_TYPE"))
		})
	})

	Context("invalid content encoding header", func() {
		It("should return bad request", func() {
			req := getRequestWithScope("testid", nil)
			req.Header.Set("Content-Encoding", "application/gzip")

			res, e := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("UNSUPPORTED_CONTENT_ENCODING"))
		})
	})

	Context("invalid scopeuuid", func() {
		It("should return bad request", func() {
			req := getRequestWithScope("wrongid", nil)

			res, e := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("UNKNOWN_SCOPE"))
		})
	})

	Context("Unitialized DB", func() {
		It("should return internal server error", func() {
			db := getDB()
			setDB(nil)

			req := getRequestWithScope("testid", nil)

			res, e := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusInternalServerError))
			Expect(e.ErrorCode).To(Equal("INTERNAL_SERVER_ERROR"))

			setDB(db)
		})
	})

	Context("bad payload", func() {
		It("should return bad request", func() {

			By("no payload")
			req := getRequestWithScope("testid", nil)
			res, e := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("Not a valid JSON payload"))

			By("payload with 0 records")
			var payload = []byte(`{}`)
			req = getRequestWithScope("testid", payload)
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("NO_RECORDS"))

			By("set content encoding to gzip but send json data")
			payload = []byte(`{
					"records":[{
						"response_status_code": 200,
						"client_id":"testapikey"
					}]
				}`)
			req = getRequestWithScope("testid", payload)
			req.Header.Set("Content-Encoding", "gzip")
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("Gzip Encoded data cannot be read"))

			By("1 bad record")
			payload = []byte(`{
						"records":[{
							"response_status_code": 200,
							"client_id":"testapikey",
							"client_received_start_timestamp": 1486406248277,
							"client_received_end_timestamp": 1486406248290
						},{
							"response_status_code": 200,
							"client_id":"testapikey"
						}]
					}`)
			req = getRequestWithScope("testid", payload)
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("MISSING_FIELD"))
		})
	})

	Context("valid payload", func() {
		It("should return successfully", func() {
			var payload = []byte(`{
					"records":[{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp": 1486406248277,
						"client_received_end_timestamp": 1486406248290
					}]
				}`)
			req := getRequestWithScope("testid", payload)
			res, _ := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusOK))
		})
	})
})

var _ = Describe("POST /analytics", func() {
	Context("invalid content type header", func() {
		It("should return bad request", func() {
			req := getRequest(nil)
			req.Header.Set("Content-Type", "application/x-gzip")
			res, e := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("UNSUPPORTED_CONTENT_TYPE"))
		})
	})

	Context("invalid content encoding header", func() {
		It("should return bad request", func() {
			req := getRequest(nil)
			req.Header.Set("Content-Encoding", "application/gzip")
			res, e := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("UNSUPPORTED_CONTENT_ENCODING"))
		})
	})

	Context("Unitialized DB", func() {
		It("should return internal server error", func() {
			db := getDB()
			setDB(nil)

			req := getRequest(nil)
			res, e := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusInternalServerError))
			Expect(e.ErrorCode).To(Equal("INTERNAL_SERVER_ERROR"))

			setDB(db)
		})
	})

	Context("bad payload", func() {
		It("should return bad request", func() {

			By("no payload")
			req := getRequest(nil)
			res, e := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("Not a valid JSON payload"))

			By("payload with no organization and environment")
			var payload = []byte(`{}`)
			req = getRequest(payload)
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("MISSING_FIELD"))

			By("payload with wrong organization and environment")
			payload = []byte(`{
					"organization":"wrongorg",
					"environment":"wrongenv",
					"records":[{
						"response_status_code": 200,
						"client_id":"testapikey"
					}]
				}`)
			req = getRequest(payload)
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("UNKNOWN_SCOPE"))

			By("payload with no records")
			payload = []byte(`{"organization":"testorg","environment":"testenv"}`)
			req = getRequest(payload)
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("NO_RECORDS"))

			By("payload with 0 records")
			payload = []byte(`{
					"organization":"testorg",
					"environment":"testenv",
					"records":[]
				}`)
			req = getRequest(payload)
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("NO_RECORDS"))

			By("payload with records not as list")
			payload = []byte(`{
					"organization":"testorg",
					"environment":"testenv",
					"records":{}
				}`)
			req = getRequest(payload)
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))

			By("payload with each record not a json in an array")
			payload = []byte(`{
					"organization":"testorg",
					"environment":"testenv",
					"records":[""]
				}`)
			req = getRequest(payload)
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))

			By("set content encoding to gzip but send json data")
			payload = []byte(`{
					"organization":"testorg",
					"environment":"testenv",
					"records":[{
						"response_status_code": 200,
						"client_id":"testapikey"
					}]
				}`)
			req = getRequest(payload)
			req.Header.Set("Content-Encoding", "gzip")
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("Gzip Encoded data cannot be read"))

			By("1 bad record")
			payload = []byte(`{
						"organization":"testorg",
						"environment":"testenv",
						"records":[{
							"response_status_code": 200,
							"client_id":"testapikey",
							"client_received_start_timestamp": 1486406248277,
							"client_received_end_timestamp": 1486406248290
						},{
							"response_status_code": 200,
							"client_id":"testapikey"
						}]
					}`)
			req = getRequest(payload)
			res, e = makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusBadRequest))
			Expect(e.ErrorCode).To(Equal("MISSING_FIELD"))
		})
	})

	Context("valid payload", func() {
		It("should return successfully", func() {
			var payload = []byte(`{
					"organization":"testorg",
					"environment":"testenv",
					"records":[{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp": 1486406248277,
						"client_received_end_timestamp": 1486406248290
					}]
				}`)
			req := getRequest(payload)
			res, _ := makeRequest(req)
			Expect(res.StatusCode).To(Equal(http.StatusOK))
		})
	})
})

func getRequest(payload []byte) *http.Request {
	uri, err := url.Parse(testServer.URL)
	uri.Path = analyticsBasePath
	Expect(err).ShouldNot(HaveOccurred())

	req, _ := http.NewRequest("POST", uri.String(), bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func getRequestWithScope(scope string, payload []byte) *http.Request {
	uri, err := url.Parse(testServer.URL)
	uri.Path = fmt.Sprintf(analyticsBasePath+"/%s", scope)
	Expect(err).ShouldNot(HaveOccurred())

	req, _ := http.NewRequest("POST", uri.String(), bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func makeRequest(req *http.Request) (*http.Response, errResponse) {
	res, err := client.Do(req)
	defer res.Body.Close()
	Expect(err).ShouldNot(HaveOccurred())

	var body errResponse
	respBody, _ := ioutil.ReadAll(res.Body)
	json.Unmarshal(respBody, &body)
	return res, body
}

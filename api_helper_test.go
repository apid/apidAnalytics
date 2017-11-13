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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

// BeforeSuite setup and AfterSuite cleanup is in apidAnalytics_suite_test.go
var _ = Describe("test getTenantFromPayload()", func() {
	Context("invalid record", func() {
		It("should return invalid record", func() {
			By("payload with missing required keys")

			var payload = []byte(`{
						"records":[{
						"response_status_code": 200,
						"client_id":"testapikey"
					}]}`)
			raw := getRaw(payload)
			_, e := getTenantFromPayload(raw)
			Expect(e.ErrorCode).To(Equal("MISSING_FIELD"))
		})
	})
	Context("valid record", func() {
		It("should return tenant with org and env", func() {
			var payload = []byte(`{
					"organization":"testorg",
					"environment":"testenv",
					"records":[{
						"response_status_code": 200,
						"client_id":"testapikey"
					}]}`)
			raw := getRaw(payload)
			tenant, _ := getTenantFromPayload(raw)
			Expect(tenant.Org).To(Equal("testorg"))
			Expect(tenant.Env).To(Equal("testenv"))
		})
	})
})

var _ = Describe("test valid() directly", func() {
	Context("invalid record", func() {
		It("should return invalid record", func() {
			By("payload with missing required keys")

			var record = []byte(`{
						"response_status_code": 200,
						"client_id":"testapikey"
					}`)
			raw := getRaw(record)
			valid, e := validate(raw)

			Expect(valid).To(BeFalse())
			Expect(e.ErrorCode).To(Equal("MISSING_FIELD"))

			By("payload with clst > clet")
			now := time.Now().Unix() * 1000
			record = []byte(`{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp":` + fmt.Sprintf("%v", now) + `,
						"client_received_end_timestamp":` + fmt.Sprintf("%v", now-60000) + `
					}`)
			raw = getRaw(record)
			valid, e = validate(raw)

			Expect(valid).To(BeFalse())
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("client_received_start_timestamp " +
				"> client_received_end_timestamp"))

			By("payload with clst = 0")
			record = []byte(`{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp": 0,
						"client_received_end_timestamp":` + fmt.Sprintf("%v", now) + `
					}`)
			raw = getRaw(record)
			valid, e = validate(raw)

			Expect(valid).To(BeFalse())
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("client_received_start_timestamp or " +
				"client_received_end_timestamp cannot be 0"))

			By("payload with clst = null")
			record = []byte(`{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp": null,
						"client_received_end_timestamp":` + fmt.Sprintf("%v", now) + `
					}`)
			raw = getRaw(record)
			valid, e = validate(raw)

			Expect(valid).To(BeFalse())
			Expect(e.ErrorCode).To(Equal("MISSING_FIELD"))

			By("payload with clst as a string")
			record = []byte(`{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp": "",
						"client_received_end_timestamp":` + fmt.Sprintf("%v", now) + `
					}`)
			raw = getRaw(record)
			valid, e = validate(raw)

			Expect(valid).To(BeFalse())
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("client_received_start_timestamp and " +
				"client_received_end_timestamp has to be number"))

			By("payload with clst after current time")
			record = []byte(`{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp":` + fmt.Sprintf("%v", now+120000) + `,
						"client_received_end_timestamp":` + fmt.Sprintf("%v", now+160000) + `
					}`)
			raw = getRaw(record)
			valid, e = validate(raw)

			Expect(valid).To(BeFalse())
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("client_received_start_timestamp " +
				"cannot be after current time"))

			By("payload with clst older than 90 days")
			starttime := time.Now().UTC().AddDate(0, 0, -95)
			clst := starttime.Unix() * 1000
			record = []byte(`{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp":` + fmt.Sprintf("%v", clst) + `,
						"client_received_end_timestamp":` + fmt.Sprintf("%v", clst+160000) + `
					}`)
			raw = getRaw(record)
			valid, e = validate(raw)

			Expect(valid).To(BeFalse())
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("client_received_start_timestamp " +
				"cannot be older than 90 days"))
		})
	})
	Context("valid record", func() {
		It("should return true", func() {
			now := time.Now().Unix() * 1000
			var record = []byte(`{
					"response_status_code": 200,
					"client_id":"testapikey",
					"client_received_start_timestamp":` + fmt.Sprintf("%v", now) + `,
					"client_received_end_timestamp":` + fmt.Sprintf("%v", now+160000) + `
				}`)
			raw := getRaw(record)
			valid, _ := validate(raw)
			Expect(valid).To(BeTrue())
		})
	})
})

var _ = Describe("test enrich() directly", func() {
	Context("enrich record where org/env in record is different from main org/env in payload", func() {
		It("The record should also have org/env for which record was validated ", func() {
			var record = []byte(`{
					"organization":"o",
					"environment":"e",
					"client_id":"testapikey",
					"client_received_start_timestamp": 1486406248277,
					"client_received_end_timestamp": 1486406248290
			}`)

			raw := getRaw(record)
			tenant := tenant{Org: "testorg", Env: "testenv"}
			enrich(raw, tenant)

			Expect(raw["organization"]).To(Equal(tenant.Org))
			Expect(raw["environment"]).To(Equal(tenant.Env))
		})
	})
	Context("enrich record where no org/env is there in the record is set", func() {
		It("developer related fields should not be added", func() {
			var record = []byte(`{
					"response_status_code": 200,
					"client_id":"testapikey",
					"client_received_start_timestamp": 1486406248277,
					"client_received_end_timestamp": 1486406248290
				}`)
			raw := getRaw(record)
			tenant := tenant{Org: "testorg", Env: "testenv"}
			enrich(raw, tenant)

			Expect(raw["organization"]).To(Equal(tenant.Org))
			Expect(raw["environment"]).To(Equal(tenant.Env))
		})
	})
	Context("enrich record where org/env is same as the main org/env in payload", func() {
		It("developer related fields should not be added", func() {
			var record = []byte(`{
					"organization":"testorg",
					"environment": "testenv",
					"client_id":"testapikey",
					"client_received_start_timestamp": 1486406248277,
					"client_received_end_timestamp": 1486406248290
			}`)
			raw := getRaw(record)
			tenant := tenant{Org: "testorg", Env: "testenv"}
			enrich(raw, tenant)

			Expect(raw["organization"]).To(Equal(tenant.Org))
			Expect(raw["environment"]).To(Equal(tenant.Env))
		})
	})
})

func getRaw(record []byte) map[string]interface{} {
	var raw map[string]interface{}

	decoder := json.NewDecoder(bytes.NewReader(record)) // Decode payload to JSON data
	decoder.UseNumber()
	err := decoder.Decode(&raw)

	Expect(err).ShouldNot(HaveOccurred())
	return raw
}

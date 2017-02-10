package apidAnalytics

import (
	"bytes"
	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// BeforeSuite setup and AfterSuite cleanup is in apidAnalytics_suite_test.go
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
			record = []byte(`{
						"response_status_code": 200,
						"client_id":"testapikey",
						"client_received_start_timestamp": 1486406248277,
						"client_received_end_timestamp": 1486406248260
					}`)
			raw = getRaw(record)
			valid, e = validate(raw)

			Expect(valid).To(BeFalse())
			Expect(e.ErrorCode).To(Equal("BAD_DATA"))
			Expect(e.Reason).To(Equal("client_received_start_timestamp > client_received_end_timestamp"))

		})
	})
	Context("valid record", func() {
		It("should return true", func() {
			var record = []byte(`{
					"response_status_code": 200,
					"client_id":"testapikey",
					"client_received_start_timestamp": 1486406248277,
					"client_received_end_timestamp": 1486406248290
				}`)
			raw := getRaw(record)
			valid, _ := validate(raw)
			Expect(valid).To(BeTrue())
		})
	})
})

var _ = Describe("test enrich() directly", func() {
	Context("enrich record for existing apiKey", func() {
		It("developer related fields should be added", func() {
			var record = []byte(`{
					"response_status_code": 200,
					"client_id":"testapikey",
					"client_received_start_timestamp": 1486406248277,
					"client_received_end_timestamp": 1486406248290
				}`)

			raw := getRaw(record)
			tenant := tenant{Org: "testorg", Env: "testenv", TenantId: "tenantid"}
			enrich(raw, "testid", tenant)

			Expect(raw["organization"]).To(Equal(tenant.Org))
			Expect(raw["environment"]).To(Equal(tenant.Env))
			Expect(raw["api_product"]).To(Equal("testproduct"))
			Expect(raw["developer_app"]).To(Equal("testapp"))
			Expect(raw["developer_email"]).To(Equal("testdeveloper@test.com"))
			Expect(raw["developer"]).To(Equal("testdeveloper"))
		})
	})

	Context("enrich record where no apikey is set", func() {
		It("developer related fields should not be added", func() {
			var record = []byte(`{
					"response_status_code": 200,
					"client_received_start_timestamp": 1486406248277,
					"client_received_end_timestamp": 1486406248290
				}`)

			raw := getRaw(record)
			tenant := tenant{Org: "testorg", Env: "testenv", TenantId: "tenantid"}
			enrich(raw, "testid", tenant)

			Expect(raw["organization"]).To(Equal(tenant.Org))
			Expect(raw["environment"]).To(Equal(tenant.Env))
			Expect(raw["api_product"]).To(BeNil())
			Expect(raw["developer_app"]).To(BeNil())
			Expect(raw["developer_email"]).To(BeNil())
			Expect(raw["developer"]).To(BeNil())
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

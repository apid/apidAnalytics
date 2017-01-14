package apidAnalytics


import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("test getTenantForScope()", func() {
	Context("get tenant for valid scopeuuid", func() {
		It("should return testorg and testenv", func() {
			tenant, dbError := getTenantForScope("testid")
			Expect(dbError.Reason).To(Equal(""))
			Expect(tenant.org).To(Equal("testorg"))
			Expect(tenant.env).To(Equal("testenv"))
			Expect(tenant.tenantId).To(Equal("tenantid"))
		})
	})

	Context("get tenant for invalid scopeuuid", func() {
		It("should return empty tenant and a db error", func() {
			tenant, dbError := getTenantForScope("wrongid")
			Expect(tenant.org).To(Equal(""))
			Expect(dbError.ErrorCode).To(Equal("UNKNOWN_SCOPE"))
		})
	})
})

var _ = Describe("test getDeveloperInfo()", func() {
	Context("get developerInfo for valid tenantId and apikey", func() {
		It("should return all right data", func() {
			developerInfo := getDeveloperInfo("tenantid","testapikey")
			Expect(developerInfo.apiProduct).To(Equal("testproduct"))
			Expect(developerInfo.developer).To(Equal("testdeveloper"))
			Expect(developerInfo.developerEmail).To(Equal("testdeveloper@test.com"))
			Expect(developerInfo.developerApp).To(Equal("testapp"))
		})
	})

	Context("get developerInfo for invalid tenantId and apikey", func() {
		It("should return all right data", func() {
			developerInfo := getDeveloperInfo("wrongid","wrongapikey")
			Expect(developerInfo.apiProduct).To(Equal(""))
			Expect(developerInfo.developer).To(Equal(""))
			Expect(developerInfo.developerEmail).To(Equal(""))
			Expect(developerInfo.developerApp).To(Equal(""))

		})
	})
})
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
			Expect(tenant.Org).To(Equal("testorg"))
			Expect(tenant.Env).To(Equal("testenv"))
			Expect(tenant.TenantId).To(Equal("tenantid"))
		})
	})

	Context("get tenant for invalid scopeuuid", func() {
		It("should return empty tenant and a db error", func() {
			tenant, dbError := getTenantForScope("wrongid")
			Expect(tenant.Org).To(Equal(""))
			Expect(dbError.ErrorCode).To(Equal("UNKNOWN_SCOPE"))
		})
	})
})

var _ = Describe("test getDeveloperInfo()", func() {
	Context("get developerInfo for valid tenantId and apikey", func() {
		It("should return all right data", func() {
			developerInfo := getDeveloperInfo("tenantid", "testapikey")
			Expect(developerInfo.ApiProduct).To(Equal("testproduct"))
			Expect(developerInfo.Developer).To(Equal("testdeveloper"))
			Expect(developerInfo.DeveloperEmail).To(Equal("testdeveloper@test.com"))
			Expect(developerInfo.DeveloperApp).To(Equal("testapp"))
		})
	})

	Context("get developerInfo for invalid tenantId and apikey", func() {
		It("should return all right data", func() {
			developerInfo := getDeveloperInfo("wrongid", "wrongapikey")
			Expect(developerInfo.ApiProduct).To(Equal(""))
			Expect(developerInfo.Developer).To(Equal(""))
			Expect(developerInfo.DeveloperEmail).To(Equal(""))
			Expect(developerInfo.DeveloperApp).To(Equal(""))
		})
	})
})

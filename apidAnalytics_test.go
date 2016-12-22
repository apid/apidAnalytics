package apidAnalytics

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("test getTenantForScope()", func() {
	Context("get tenant for valid scopeuuid", func() {
		It("should return testorg and testenv", func() {
			tenant, dbError := getTenantForScope("testid")
			Expect(dbError.reason).To(Equal(""))
			Expect(tenant.org).To(Equal("testorg"))
			Expect(tenant.env).To(Equal("testenv"))
		})
	})

	Context("get tenant for invalid scopeuuid", func() {
		It("should return empty tenant and a db error", func() {
			tenant, dbError := getTenantForScope("wrongid")
			Expect(tenant.org).To(Equal(""))
			Expect(dbError.errorCode).To(Equal("UNKNOWN_SCOPE"))
		})
	})
})
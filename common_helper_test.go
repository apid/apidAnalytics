package apidAnalytics

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("test getTenantForScope()", func() {
	Context("with usecaching set to true", func() {
		Context("get tenant for valid scopeuuid", func() {
			It("should return testorg and testenv", func() {
				tenant, dbError := getTenantForScope("testid")
				Expect(dbError.Reason).To(Equal(""))
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

	Context("with usecaching set to false", func() {
		BeforeEach(func() {
			config.Set(useCaching, false)
		})
		AfterEach(func() {
			config.Set(useCaching, true)
		})
		Context("get tenant for valid scopeuuid", func() {
			It("should return testorg and testenv", func() {
				tenant, dbError := getTenantForScope("testid")
				Expect(dbError.Reason).To(Equal(""))
				Expect(tenant.Org).To(Equal("testorg"))
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
})

var _ = Describe("test getDeveloperInfo()", func() {
	Context("with usecaching set to true", func() {
		Context("get developerInfo for valid tenantId and apikey", func() {
			It("should return all right data", func() {
				developerInfo := getDeveloperInfo("tenantid", "testapikey")
				Expect(developerInfo.ApiProduct).To(Equal("testproduct"))
				Expect(developerInfo.Developer).To(Equal("testdeveloper"))
			})
		})

		Context("get developerInfo for invalid tenantId and apikey", func() {
			It("should return all right data", func() {
				developerInfo := getDeveloperInfo("wrongid", "wrongapikey")
				Expect(developerInfo.ApiProduct).To(Equal(""))
			})
		})
	})

	Context("with usecaching set to false", func() {
		BeforeEach(func() {
			config.Set(useCaching, false)
		})
		AfterEach(func() {
			config.Set(useCaching, true)
		})
		Context("get developerInfo for valid tenantId and apikey", func() {
			It("should return all right data", func() {
				developerInfo := getDeveloperInfo("tenantid", "testapikey")
				Expect(developerInfo.ApiProduct).To(Equal("testproduct"))
				Expect(developerInfo.Developer).To(Equal("testdeveloper"))
			})
		})
		Context("get developerInfo for invalid tenantId and apikey", func() {
			It("should return all right data", func() {
				developerInfo := getDeveloperInfo("wrongid", "wrongapikey")
				Expect(developerInfo.ApiProduct).To(Equal(""))
			})
		})
	})
})

var _ = Describe("test getValuesIgnoringNull()", func() {
	Context("Null sql value", func() {
		It("should return empty string", func() {
			a := sql.NullString{String: "null", Valid: false}
			res := getValuesIgnoringNull(a)
			Expect(res).To(Equal(""))
		})
	})
	Context("not null sql value", func() {
		It("should return  string", func() {
			a := sql.NullString{String: "sql", Valid: true}
			res := getValuesIgnoringNull(a)
			Expect(res).To(Equal("sql"))
		})
	})
})

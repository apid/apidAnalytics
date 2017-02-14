package apidAnalytics

import (
	"database/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/apigee-labs/transicator/common"
)

var _ = Describe("test getTenantForScope()", func() {
	Context("with usecaching set to true", func() {
		BeforeEach(func() {
			config.Set(useCaching, true)
			snapshot := getDatascopeSnapshot()
			createTenantCache(&snapshot)
			Expect(len(tenantCache)).To(Equal(1))
		})
		AfterEach(func() {
			config.Set(useCaching, false)
		})
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

var _ = Describe("test getTenantFromDB()", func() {
	Context("get developerInfo for valid scopeuuid", func() {
		It("should return all right data", func() {
			tenant, dbError := getTenantFromDB("testid")
			Expect(dbError.Reason).To(Equal(""))
			Expect(tenant.TenantId).To(Equal("tenantid"))
		})
	})
	Context("get developerInfo for invalid scopeuuid", func() {
		It("should return error", func() {
			tenant, dbError := getTenantFromDB("wrongid")
			Expect(tenant.Org).To(Equal(""))
			Expect(dbError.ErrorCode).To(Equal("UNKNOWN_SCOPE"))
		})
	})

})

var _ = Describe("test getDeveloperInfo()", func() {
	Context("with usecaching set to true", func() {
		BeforeEach(func() {
			config.Set(useCaching, true)
			updateDeveloperInfoCache()
		})
		AfterEach(func() {
			config.Set(useCaching, false)
		})
		Context("get developerInfo for valid tenantId and apikey", func() {
			It("should return all right data", func() {
				key := getKeyForDeveloperInfoCache("tenantid", "testapikey")
				_, e := developerInfoCache[key]
				Expect(e).To(BeFalse())

				getDeveloperInfo("tenantid", "testapikey")
				devInfo, e := developerInfoCache[key]
				Expect(e).To(BeTrue())
				Expect(devInfo.ApiProduct).To(Equal("testproduct"))
				Expect(devInfo.Developer).To(Equal("testdeveloper"))
			})
		})

		Context("get developerInfo for invalid tenantId and apikey", func() {
			It("should return all empty", func() {
				developerInfo := getDeveloperInfo("wrongid", "wrongapikey")
				Expect(developerInfo.ApiProduct).To(Equal(""))
			})
		})
	})

	Context("with usecaching set to false", func() {
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

var _ = Describe("test getDevInfoFromDB()", func() {
	Context("get developerInfo for valid tenantId and apikey", func() {
		It("should return all right data", func() {
			developerInfo, err := getDevInfoFromDB("tenantid", "testapikey")
			Expect(err).ToNot(HaveOccurred())
			Expect(developerInfo.ApiProduct).To(Equal("testproduct"))
			Expect(developerInfo.Developer).To(Equal("testdeveloper"))
		})
	})
	Context("get developerInfo for invalid tenantId and apikey", func() {
		It("should return all empty data", func() {
			developerInfo, err := getDevInfoFromDB("wrongid", "wrongapikey")
			Expect(err).To(HaveOccurred())
			Expect(developerInfo.ApiProduct).To(Equal(""))
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

func getDatascopeSnapshot() common.Snapshot {
	event := common.Snapshot{
		SnapshotInfo: "test_snapshot_valid",
		Tables: []common.Table{
			{
				Name: LISTENER_TABLE_DATA_SCOPE,
				Rows: []common.Row{
					{
						"id":    &common.ColumnVal{Value: "testid"},
						"scope": &common.ColumnVal{Value: "tenantid"},
						"org":   &common.ColumnVal{Value: "testorg"},
						"env":   &common.ColumnVal{Value: "testenv"},
					},
				},
			},
		},
	}
	return event
}

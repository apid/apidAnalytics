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
	Context("get tenant for valid scopeuuid", func() {
		It("should return testorg and testenv", func() {
			tenant, dbError := getTenantFromDB("testid")
			Expect(dbError.Reason).To(Equal(""))
			Expect(tenant.TenantId).To(Equal("tenantid"))
			Expect(tenant.Org).To(Equal("testorg"))
			Expect(tenant.Env).To(Equal("testenv"))

		})
	})
	Context("get tenant for invalid scopeuuid", func() {
		It("should return error with unknown scope", func() {
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

var _ = Describe("test validateTenant()", func() {
	Context("validate tenant for org/env that exists in DB", func() {
		It("should not return an error and tenantId should be populated", func() {
			tenant := tenant{Org: "testorg", Env: "testenv"}
			valid, dbError := validateTenant(&tenant)
			Expect(valid).To(BeTrue())
			Expect(tenant.TenantId).To(Equal("tenantid"))
			Expect(dbError.ErrorCode).To(Equal(""))
		})
	})
	Context("validate tenant for org/env that do not exist in DB", func() {
		It("should return error with unknown_scope", func() {
			tenant := tenant{Org: "wrongorg", Env: "wrongenv"}
			valid, dbError := validateTenant(&tenant)
			Expect(valid).To(BeFalse())
			Expect(dbError.ErrorCode).To(Equal("UNKNOWN_SCOPE"))
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

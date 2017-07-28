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
			createOrgEnvCache(&snapshot)
			Expect(len(tenantCache)).To(Equal(1))
			Expect(len(orgEnvCache)).To(Equal(1))
		})
		AfterEach(func() {
			config.Set(useCaching, false)
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

var _ = Describe("test validateTenant()", func() {
	Context("with usecaching set to true", func() {
		BeforeEach(func() {
			config.Set(useCaching, true)
			snapshot := getDatascopeSnapshot()
			createOrgEnvCache(&snapshot)
			Expect(len(orgEnvCache)).To(Equal(1))
		})
		AfterEach(func() {
			config.Set(useCaching, false)
		})
		Context("valididate existing org/env", func() {
			It("should return true", func() {
				tenant := tenant{Org: "testorg", Env: "testenv"}
				valid, dbError := validateTenant(tenant)
				Expect(dbError.Reason).To(Equal(""))
				Expect(valid).To(BeTrue())
			})
		})

		Context("get tenant for invalid scopeuuid", func() {
			It("should return false", func() {
				tenant := tenant{Org: "wrongorg", Env: "wrongenv"}
				valid, dbError := validateTenant(tenant)
				Expect(dbError.ErrorCode).To(Equal("UNKNOWN_SCOPE"))
				Expect(valid).To(BeFalse())
			})
		})
	})
	Context("with usecaching set to false", func() {
		Context("valididate existing org/env", func() {
			It("should return true", func() {
				tenant := tenant{Org: "testorg", Env: "testenv"}
				valid, dbError := validateTenant(tenant)
				Expect(dbError.Reason).To(Equal(""))
				Expect(valid).To(BeTrue())
			})
		})
		Context("get tenant for invalid scopeuuid", func() {
			It("should return false", func() {
				tenant := tenant{Org: "wrongorg", Env: "wrongenv"}
				valid, dbError := validateTenant(tenant)
				Expect(dbError.ErrorCode).To(Equal("UNKNOWN_SCOPE"))
				Expect(valid).To(BeFalse())
			})
		})
	})
})

var _ = Describe("test validateTenantFromDB()", func() {
	Context("validate tenant for org/env that exists in DB", func() {
		It("should not return an error and valid should be true", func() {
			tenant := tenant{Org: "testorg", Env: "testenv"}
			valid, dbError := validateTenantFromDB(tenant)
			Expect(valid).To(BeTrue())
			Expect(dbError.ErrorCode).To(Equal(""))
		})
	})
	Context("validate tenant for org/env that do not exist in DB", func() {
		It("should return error with unknown_scope", func() {
			tenant := tenant{Org: "wrongorg", Env: "wrongenv"}
			valid, dbError := validateTenantFromDB(tenant)
			Expect(valid).To(BeFalse())
			Expect(dbError.ErrorCode).To(Equal("UNKNOWN_SCOPE"))
		})
	})

})

var _ = Describe("test getKeyForOrgEnvCache()", func() {
	It("should return key using org and env", func() {
		res := getKeyForOrgEnvCache("testorg", "testenv")
		Expect(res).To(Equal("testorg~testenv"))
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

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
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
)

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	LISTENER_TABLE_DATA_SCOPE = "edgex.data_scope"
)

var _ = Describe("ApigeeSync event", func() {

	handler := handler{}

	Context("ApigeeSync snapshot event", func() {
		var db apid.DB
		var snapshot common.Snapshot

		BeforeEach(func() {
			db = getDB()
			snapshot = common.Snapshot{SnapshotInfo: "test_snapshot"}
		})

		AfterEach(func() {
			setDB(db)
		})

		It("should set DB to appropriate version", func() {
			handler.Handle(&snapshot)

			expectedDB, err := data.DBVersion(snapshot.SnapshotInfo)
			Expect(err).NotTo(HaveOccurred())

			Expect(getDB() == expectedDB).Should(BeTrue())
		})
	})

	Context("Process changeList", func() {
		Context(LISTENER_TABLE_DATA_SCOPE, func() {
			BeforeEach(func() {
				config.Set(useCaching, true)
				createTenantCache()
				Expect(len(tenantCache)).To(Equal(1))
				createOrgEnvCache()
				Expect(len(orgEnvCache)).To(Equal(1))
			})

			AfterEach(func() {
				config.Set(useCaching, false)
			})

			It("insert/delete event should add/remove to/from cache if usecaching is true", func() {
				insert := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_DATA_SCOPE,
							NewRow: common.Row{
								"id":               &common.ColumnVal{Value: "i2"},
								"_change_selector": &common.ColumnVal{Value: "c2"},
								"apid_cluster_id":  &common.ColumnVal{Value: "a2"},
								"scope":            &common.ColumnVal{Value: "s2"},
								"org":              &common.ColumnVal{Value: "o2"},
								"env":              &common.ColumnVal{Value: "e2"},
								"created":          &common.ColumnVal{Value: "c2"},
								"created_by":       &common.ColumnVal{Value: "c2"},
								"updated":          &common.ColumnVal{Value: "u2"},
								"updated_by":       &common.ColumnVal{Value: "u2"},
							},
						},
					},
				}

				handler.Handle(&insert)
				tenant := tenantCache["i2"]
				Expect(tenant.Org).To(Equal("o2"))
				Expect(tenant.Env).To(Equal("e2"))

				orgEnv := getKeyForOrgEnvCache("o2", "e2")
				Expect(orgEnvCache[orgEnv]).To(BeTrue())

				delete := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Delete,
							Table:     LISTENER_TABLE_DATA_SCOPE,
							OldRow:    insert.Changes[0].NewRow,
						},
					},
				}

				handler.Handle(&delete)
				_, exists := tenantCache["i2"]
				Expect(exists).To(BeFalse())

				_, exists = orgEnvCache[orgEnv]
				Expect(exists).To(BeFalse())
			})
		})
	})
})

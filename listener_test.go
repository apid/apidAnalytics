package apidAnalytics

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/apigee-labs/transicator/common"
)

const (
	LISTENER_TABLE_APP_CRED_MAPPER = "kms.app_credential_apiproduct_mapper"
	LISTENER_TABLE_DATA_SCOPE      = "edgex.data_scope"
)

var _ = Describe("listener", func() {
	Context("Process changeList", func() {
		Context(LISTENER_TABLE_DATA_SCOPE, func() {
			It("insert/delete event should add/remove to/from cache if usecaching is true", func() {
				config.Set(useCaching, true)
				txn, err := getDB().Begin()
				Expect(err).ShouldNot(HaveOccurred())
				txn.Exec("INSERT INTO DATA_SCOPE (id, _change_selector, apid_cluster_id, scope, org, env) "+
					"VALUES"+
					"($1,$2,$3,$4,$5,$6)",
					"i2",
					"c2",
					"a2",
					"s2",
					"o2",
					"e2",
				)
				txn.Commit()

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

				processChange(&insert)
				tenant := tenantCache["i2"]
				Expect(tenant.TenantId).To(Equal("s2"))
				Expect(tenant.Org).To(Equal("o2"))
				Expect(tenant.Env).To(Equal("e2"))

				txn, err = getDB().Begin()
				Expect(err).ShouldNot(HaveOccurred())
				txn.Exec("DELETE FROM DATA_SCOPE where id = 'i2'")
				txn.Commit()

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

				processChange(&delete)
				_, exists := tenantCache["i2"]
				Expect(exists).To(Equal(false))

			})
		})
		Context(LISTENER_TABLE_APP_CRED_MAPPER, func() {
			It("insert/delete event should refresh developer cache if usecaching is true", func() {
				config.Set(useCaching, true)

				txn, err := getDB().Begin()
				Expect(err).ShouldNot(HaveOccurred())
				txn.Exec("INSERT INTO APP_CREDENTIAL_APIPRODUCT_MAPPER (tenant_id, appcred_id, app_id, apiprdt_id, status, _change_selector) "+
					"VALUES"+
					"($1,$2,$3,$4,$5,$6)",
					"tenantid",
					"aci",
					"ai",
					"testproductid",
					"APPROVED",
					"12345",
				)

				txn.Exec("INSERT INTO APP (id, tenant_id, name, developer_id) "+
					"VALUES"+
					"($1,$2,$3,$4)",
					"ai",
					"tenantid",
					"name",
					"testdeveloperid",
				)
				txn.Commit()

				insert := common.ChangeList{
					LastSequence: "test",
					Changes: []common.Change{
						{
							Operation: common.Insert,
							Table:     LISTENER_TABLE_APP_CRED_MAPPER,
							NewRow: common.Row{
								"tenant_id":        &common.ColumnVal{Value: "tenantid"},
								"appcred_id":       &common.ColumnVal{Value: "aci"},
								"app_id":           &common.ColumnVal{Value: "ai"},
								"apiprdt_id":       &common.ColumnVal{Value: "api"},
								"status":           &common.ColumnVal{Value: "s"},
								"_change_selector": &common.ColumnVal{Value: "c"},
							},
						},
					},
				}

				processChange(&insert)
				key := getKeyForDeveloperInfoCache("tenantid", "aci")
				developerInfo := developerInfoCache[key]

				Expect(developerInfo.ApiProduct).To(Equal("testproduct"))
				Expect(developerInfo.Developer).To(Equal("testdeveloper"))
				Expect(developerInfo.DeveloperEmail).To(Equal("testdeveloper@test.com"))
				Expect(developerInfo.DeveloperApp).To(Equal("name"))
			})
		})
	})
})

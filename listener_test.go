package apidAnalytics

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
)

const (
	LISTENER_TABLE_DATA_SCOPE = "edgex.data_scope"
)

var _ = Describe("ApigeeSync event", func() {

	var db apid.DB
	handler := handler{}

	BeforeEach(func() {
		db = getDB()

		config.Set(useCaching, true)

		snapshot := getDatascopeSnapshot()
		createTenantCache(&snapshot)
		Expect(len(tenantCache)).To(Equal(1))
	})

	AfterEach(func() {
		config.Set(useCaching, false)
		setDB(db)
	})

	Context("ApigeeSync snapshot event", func() {
		It("should set DB to appropriate version", func() {
			config.Set(useCaching, false)

			event := common.Snapshot{
				SnapshotInfo: "test_snapshot",
				Tables:       []common.Table{},
			}

			handler.Handle(&event)

			expectedDB, err := data.DBVersion(event.SnapshotInfo)
			Expect(err).NotTo(HaveOccurred())

			Expect(getDB() == expectedDB).Should(BeTrue())
		})

		It("should process a valid Snapshot", func() {
			event := common.Snapshot{
				SnapshotInfo: "test_snapshot_valid",
				Tables: []common.Table{
					{
						Name: LISTENER_TABLE_DATA_SCOPE,
						Rows: []common.Row{
							{
								"id":               &common.ColumnVal{Value: "i"},
								"_change_selector": &common.ColumnVal{Value: "c"},
								"apid_cluster_id":  &common.ColumnVal{Value: "a"},
								"scope":            &common.ColumnVal{Value: "s"},
								"org":              &common.ColumnVal{Value: "o"},
								"env":              &common.ColumnVal{Value: "e"},
								"created":          &common.ColumnVal{Value: "c"},
								"created_by":       &common.ColumnVal{Value: "c"},
								"updated":          &common.ColumnVal{Value: "u"},
								"updated_by":       &common.ColumnVal{Value: "u"},
							},
						},
					},
				},
			}

			handler.Handle(&event)
			tenant := tenantCache["i"]
			Expect(tenant.TenantId).To(Equal("s"))
			Expect(tenant.Org).To(Equal("o"))
			Expect(tenant.Env).To(Equal("e"))
		})
	})

	Context("Process changeList", func() {
		Context(LISTENER_TABLE_DATA_SCOPE, func() {
			It("insert/delete event should add/remove to/from cache if usecaching is true", func() {
				txn, err := getDB().Begin()
				Expect(err).ShouldNot(HaveOccurred())
				txn.Exec("INSERT INTO edgex_data_scope (id, _change_selector, apid_cluster_id, scope, org, env) "+
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

				handler.Handle(&insert)
				tenant := tenantCache["i2"]
				Expect(tenant.TenantId).To(Equal("s2"))
				Expect(tenant.Org).To(Equal("o2"))
				Expect(tenant.Env).To(Equal("e2"))

				txn, err = getDB().Begin()
				Expect(err).ShouldNot(HaveOccurred())
				txn.Exec("DELETE FROM edgex_data_scope where id = 'i2'")
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

				handler.Handle(&delete)
				_, exists := tenantCache["i2"]
				Expect(exists).To(BeFalse())
			})
		})
	})
})

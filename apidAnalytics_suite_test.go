package apidAnalytics

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/30x/apid"
	"github.com/30x/apid/factory"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
)

var (
	testTempDir string
	testServer  *httptest.Server
	unsafeDB    apid.DB
	dbMux       sync.RWMutex
)

func TestApidAnalytics(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApidAnalytics Suite")
}

var _ = BeforeSuite(func() {
	apid.Initialize(factory.DefaultServicesFactory())

	config := apid.Config()

	var err error
	testTempDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())

	config.Set("data_path", testTempDir)
	config.Set(uapEndpoint, "http://localhost:9000") // dummy value

	apid.InitializePlugins()

	db, err := apid.Data().DB()
	Expect(err).NotTo(HaveOccurred())
	setDB(db)
	createApidClusterTables(db)
	insertTestData(db)
	testServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == analyticsBasePathDefault {
			saveAnalyticsRecord(w, req)
		}
	}))
})

func setDB(db apid.DB) {
	dbMux.Lock()
	unsafeDB = db
	dbMux.Unlock()
}

func createApidClusterTables(db apid.DB) {
	_, err := db.Exec(`
CREATE TABLE apid_cluster (
    id text,
    instance_id text,
    name text,
    description text,
    umbrella_org_app_name text,
    created int64,
    created_by text,
    updated int64,
    updated_by text,
    _change_selector text,
    snapshotInfo text,
    lastSequence text,
    PRIMARY KEY (id)
);
CREATE TABLE data_scope (
    id text,
    apid_cluster_id text,
    scope text,
    org text,
    env text,
    created int64,
    created_by text,
    updated int64,
    updated_by text,
    _change_selector text,
    PRIMARY KEY (id)
);
`)
	if err != nil {
		log.Panic("Unable to initialize DB", err)
	}
}

func insertTestData(db apid.DB) {

	txn, err := db.Begin()
	Expect(err).ShouldNot(HaveOccurred())

	txn.Exec("INSERT INTO DATA_SCOPE (id, _change_selector, apid_cluster_id, scope, org, env) "+
		"VALUES"+
		"($1,$2,$3,$4,$5,$6)",
		"testid",
		"some_cluster_id",
		"some_cluster_id",
		"tenant_id_xxxx",
		"testorg",
		"testenv",
	)
	txn.Commit()
	var count int64
	db.QueryRow("select count(*) from data_scope").Scan(&count)
}

var _ = AfterSuite(func() {
	apid.Events().Close()
	if testServer != nil {
		testServer.Close()
	}
	os.RemoveAll(testTempDir)
})

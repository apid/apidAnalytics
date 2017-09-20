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

	"encoding/json"
	"github.com/apid/apid-core"
	"github.com/apid/apid-core/factory"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

var (
	testTempDir string
	testServer  *httptest.Server
)

func TestApidAnalytics(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApidAnalytics Suite")
}

var _ = BeforeSuite(func() {
	apid.Initialize(factory.DefaultServicesFactory())

	config = apid.Config()

	var err error
	testTempDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())

	config.Set("local_storage_path", testTempDir)
	config.Set("data_path", testTempDir)
	config.Set("apigeesync_apid_instance_id", "abcdefgh-ijkl-mnop-qrst-uvwxyz123456") // dummy value

	db, err := apid.Data().DB()
	Expect(err).NotTo(HaveOccurred())
	initDb(db)

	config.Set(uapServerBase, "http://localhost:9000") // dummy value
	apid.InitializePlugins("")

	// Analytics POST API
	router := apid.API().Router()
	router.HandleFunc(analyticsBasePath+"/{bundle_scope_uuid}", func(w http.ResponseWriter, req *http.Request) {
		saveAnalyticsRecord(w, req)
	}).Methods("POST")

	// Mock UAP collection endpoint
	router.HandleFunc("/analytics", func(w http.ResponseWriter, req *http.Request) {
		mockUAPCollection(w, req)
	}).Methods("GET")

	// fake AWS S3
	router.HandleFunc("/upload", func(w http.ResponseWriter, req *http.Request) {
		mockFinalDatastore(w, req)
	}).Methods("PUT")

	testServer = httptest.NewServer(router)

	// ser serverbased to local so that responses can be mocked
	config.Set(uapServerBase, testServer.URL)
})

func mockUAPCollection(w http.ResponseWriter, req *http.Request) {
	if req.Header.Get("Authorization") == "" {
		w.WriteHeader(http.StatusUnauthorized)
	} else {
		if req.URL.Query().Get("tenant") == "testorg~testenv" {
			w.WriteHeader(http.StatusOK)

			body := make(map[string]interface{})
			body["url"] = testServer.URL + "/upload?awskey=xxxx"
			bytes, _ := json.Marshal(body)
			w.Write(bytes)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func mockFinalDatastore(w http.ResponseWriter, req *http.Request) {
	status := req.URL.Query().Get("expected_status")
	switch status {
	case "ok":
		w.WriteHeader(http.StatusOK)
	case "serverError":
		w.WriteHeader(http.StatusInternalServerError)
	case "forbidden":
		w.WriteHeader(http.StatusForbidden)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func initDb(db apid.DB) {
	createApidClusterTables(db)
	createTables(db)
	insertTestData(db)
	setDB(db)
}

func createTables(db apid.DB) {
	tx, err := db.Begin()
	Expect(err).Should(Succeed())
	defer tx.Rollback()
	_, err = tx.Exec(`
		CREATE TABLE IF NOT EXISTS kms_api_product (
		    id text,
		    tenant_id text,
		    name text,
		    display_name text,
		    description text,
		    api_resources text[],
		    approval_type text,
		    _change_selector text,
		    proxies text[],
		    environments text[],
		    quota text,
		    quota_time_unit text,
		    quota_interval int,
		    created_at int64,
		    created_by text,
		    updated_at int64,
		    updated_by text,
		    PRIMARY KEY (tenant_id, id));
		CREATE TABLE IF NOT EXISTS kms_developer (
		    id text,
		    tenant_id text,
		    username text,
		    first_name text,
		    last_name text,
		    password text,
		    email text,
		    status text,
		    encrypted_password text,
		    salt text,
		    _change_selector text,
		    created_at int64,
		    created_by text,
		    updated_at int64,
		    updated_by text,
		    PRIMARY KEY (tenant_id, id)
		);
		CREATE TABLE IF NOT EXISTS kms_app (
		    id text,
		    tenant_id text,
		    name text,
		    display_name text,
		    access_type text,
		    callback_url text,
		    status text,
		    app_family text,
		    company_id text,
		    developer_id text,
		    type int,
		    created_at int64,
		    created_by text,
		    updated_at int64,
		    updated_by text,
		    _change_selector text,
		    PRIMARY KEY (tenant_id, id)
		);
		CREATE TABLE IF NOT EXISTS kms_app_credential_apiproduct_mapper (
		    tenant_id text,
		    appcred_id text,
		    app_id text,
		    apiprdt_id text,
		    _change_selector text,
		    status text,
		    PRIMARY KEY (appcred_id, app_id, apiprdt_id,tenant_id)
		);
	`)
	Expect(err).Should(Succeed())
	err = tx.Commit()
	Expect(err).Should(Succeed())
}

func createApidClusterTables(db apid.DB) {
	txn, err := db.Begin()
	Expect(err).Should(Succeed())
	defer txn.Rollback()
	_, err = txn.Exec(`
		CREATE TABLE edgex_apid_cluster (
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
		CREATE TABLE edgex_data_scope (
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
	Expect(err).Should(Succeed())
	err = txn.Commit()
	Expect(err).Should(Succeed())
}

func insertTestData(db apid.DB) {

	txn, err := db.Begin()
	Expect(err).Should(Succeed())
	defer txn.Rollback()
	_, err = txn.Exec("INSERT INTO kms_app_credential_apiproduct_mapper (tenant_id,"+
		" appcred_id, app_id, apiprdt_id, status, _change_selector) "+
		"VALUES"+
		"($1,$2,$3,$4,$5,$6)",
		"tenantid",
		"testapikey",
		"testappid",
		"testproductid",
		"APPROVED",
		"12345",
	)
	Expect(err).Should(Succeed())
	_, err = txn.Exec("INSERT INTO kms_app (id, tenant_id, name, developer_id) "+
		"VALUES"+
		"($1,$2,$3,$4)",
		"testappid",
		"tenantid",
		"testapp",
		"testdeveloperid",
	)
	Expect(err).Should(Succeed())
	_, err = txn.Exec("INSERT INTO kms_api_product (id, tenant_id, name) "+
		"VALUES"+
		"($1,$2,$3)",
		"testproductid",
		"tenantid",
		"testproduct",
	)
	Expect(err).Should(Succeed())
	_, err = txn.Exec("INSERT INTO kms_developer (id, tenant_id, username, email) "+
		"VALUES"+
		"($1,$2,$3,$4)",
		"testdeveloperid",
		"tenantid",
		"testdeveloper",
		"testdeveloper@test.com",
	)
	Expect(err).Should(Succeed())
	_, err = txn.Exec("INSERT INTO edgex_data_scope (id, _change_selector, "+
		"apid_cluster_id, scope, org, env) "+
		"VALUES"+
		"($1,$2,$3,$4,$5,$6)",
		"testid",
		"some_change_selector",
		"some_cluster_id",
		"tenantid",
		"testorg",
		"testenv",
	)
	Expect(err).Should(Succeed())
	err = txn.Commit()
	Expect(err).Should(Succeed())
}

var _ = AfterSuite(func() {
	apid.Events().Close()
	if testServer != nil {
		testServer.Close()
	}
	os.RemoveAll(testTempDir)
})

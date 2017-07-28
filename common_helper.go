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
	"github.com/apigee-labs/transicator/common"
	"sync"
)

// Cache for scope uuid to org, env and tenantId information
var tenantCache map[string]tenant

// RW lock for tenant map cache since the cache can be
// read while its being written to and vice versa
var tenantCachelock = sync.RWMutex{}

// Cache for all org/env for this cluster
var orgEnvCache map[string]bool

// RW lock for orgEnvCache map cache since the cache can be
// read while its being written to and vice versa
var orgEnvCacheLock = sync.RWMutex{}

// Cache for apiKey~tenantId to developer related information
var developerInfoCache map[string]developerInfo

// RW lock for developerInfo map cache since the cache can be
// read while its being written to and vice versa
var developerInfoCacheLock = sync.RWMutex{}

// Load data scope information into an in-memory cache so that
// for each record a DB lookup is not required
func createTenantCache(snapshot *common.Snapshot) {
	// Lock before writing to the map as it has multiple readers
	tenantCachelock.Lock()
	defer tenantCachelock.Unlock()
	tenantCache = make(map[string]tenant)

	for _, table := range snapshot.Tables {
		switch table.Name {
		case "edgex.data_scope":
			for _, row := range table.Rows {
				var org, env, tenantId, id string

				row.Get("id", &id)
				row.Get("scope", &tenantId)
				row.Get("org", &org)
				row.Get("env", &env)
				if id != "" {
					tenantCache[id] = tenant{Org: org,
						Env:      env,
						TenantId: tenantId}
				}
			}
		}
	}
	log.Debugf("Count of data scopes in the cache: %d", len(tenantCache))
}

// Load data scope information into an in-memory cache so that
// for each record a DB lookup is not required
func createOrgEnvCache(snapshot *common.Snapshot) {
	// Lock before writing to the map as it has multiple readers
	orgEnvCacheLock.Lock()
	defer orgEnvCacheLock.Unlock()
	orgEnvCache = make(map[string]bool)

	for _, table := range snapshot.Tables {
		switch table.Name {
		case "edgex.data_scope":
			for _, row := range table.Rows {
				var org, env string

				row.Get("org", &org)
				row.Get("env", &env)
				orgEnv := getKeyForOrgEnvCache(org, env)
				if orgEnv != "" {
					orgEnvCache[orgEnv] = true
				}
			}
		}
	}
	log.Debugf("Count of org~env in the cache: %d", len(orgEnvCache))
}

// Load data scope information into an in-memory cache so that
// for each record a DB lookup is not required
func updateDeveloperInfoCache() {
	// Lock before writing to the map as it has multiple readers
	developerInfoCacheLock.Lock()
	defer developerInfoCacheLock.Unlock()
	developerInfoCache = make(map[string]developerInfo)
	log.Debug("Invalidated developerInfo cache")
}

// Returns Tenant Info given a scope uuid from the cache or by querying
// the DB directly based on useCachig config
func getTenantForScope(scopeuuid string) (tenant, dbError) {
	if config.GetBool(useCaching) {
		// acquire a read lock as this cache has 1 writer as well
		tenantCachelock.RLock()
		ten, exists := tenantCache[scopeuuid]
		tenantCachelock.RUnlock()
		dbErr := dbError{}

		if !exists {
			log.Debugf("No tenant found for scopeuuid = %s "+
				"in cache", scopeuuid)
			log.Debug("loading info from DB")

			// Update cache
			t, err := getTenantFromDB(scopeuuid)

			if err.ErrorCode != "" {
				dbErr = err
				ten = t
			} else {
				// update cache
				tenantCachelock.Lock()
				defer tenantCachelock.Unlock()
				tenantCache[scopeuuid] = t
				ten = t
			}
		}
		return ten, dbErr
	} else {
		return getTenantFromDB(scopeuuid)
	}
}

// Returns Developer related info given an apiKey and tenantId
// from the cache or by querying the DB directly based on useCachig config
func getDeveloperInfo(tenantId string, apiKey string) developerInfo {
	if config.GetBool(useCaching) {
		keyForMap := getKeyForDeveloperInfoCache(tenantId, apiKey)
		// acquire a read lock as this cache has 1 writer as well
		developerInfoCacheLock.RLock()
		devInfo, exists := developerInfoCache[keyForMap]
		developerInfoCacheLock.RUnlock()

		if !exists {
			log.Debugf("No data found for for tenantId = %s"+
				" and apiKey = %s in cache", tenantId, apiKey)
			log.Debug("loading info from DB")

			// Update cache
			dev, err := getDevInfoFromDB(tenantId, apiKey)

			if err == nil {
				// update cache
				developerInfoCacheLock.Lock()
				defer developerInfoCacheLock.Unlock()
				key := getKeyForDeveloperInfoCache(tenantId, apiKey)
				developerInfoCache[key] = dev
			}

			devInfo = dev

		}
		return devInfo
	} else {
		devInfo, _ := getDevInfoFromDB(tenantId, apiKey)
		return devInfo
	}
}

// Returns tenant info by querying DB directly
func getTenantFromDB(scopeuuid string) (tenant, dbError) {
	var org, env, tenantId string

	db := getDB()
	error := db.QueryRow("SELECT env, org, scope FROM edgex_data_scope"+
		" where id = ?", scopeuuid).Scan(&env, &org, &tenantId)

	switch {
	case error == sql.ErrNoRows:
		reason := "No tenant found for this scopeuuid: " + scopeuuid
		errorCode := "UNKNOWN_SCOPE"
		return tenant{}, dbError{
			ErrorCode: errorCode,
			Reason:    reason}
	case error != nil:
		reason := error.Error()
		errorCode := "INTERNAL_SEARCH_ERROR"
		return tenant{}, dbError{
			ErrorCode: errorCode,
			Reason:    reason}
	}
	return tenant{
		Org:      org,
		Env:      env,
		TenantId: tenantId}, dbError{}
}

/*
Checks if given org/env exists is a valid scope for this apid cluster
It also stores the scope i.e. tenant_id in the tenant object using pointer.
tenant_id in combination with apiKey is used to find kms related information
*/
func validateTenant(tenant *tenant) (bool, dbError) {
	if config.GetBool(useCaching) {
		// acquire a read lock as this cache has 1 writer as well
		orgEnvCacheLock.RLock()
		orgEnv := getKeyForOrgEnvCache(tenant.Org, tenant.Env)
		_, exists := orgEnvCache[orgEnv]
		orgEnvCacheLock.RUnlock()
		dbErr := dbError{}
		if !exists {
			log.Debugf("OrgEnv = %s not found "+
				"in cache", orgEnv)
			log.Debug("loading info from DB")

			// Update cache
			valid, dbErr := validateTenantFromDB(tenant)
			if valid {
				// update cache
				orgEnvCacheLock.Lock()
				defer orgEnvCacheLock.Unlock()
				orgEnvCache[orgEnv] = true
			}
			return valid, dbErr
		} else {
			return true, dbErr
		}
	} else {
		return validateTenantFromDB(tenant)
	}

}

func validateTenantFromDB(tenant *tenant) (bool, dbError) {
	db := getDB()
	error := db.QueryRow("SELECT scope FROM edgex_data_scope"+
		" where org = ? and env = ?", &tenant.Org, &tenant.Env).Scan(&tenant.TenantId)
	switch {
	case error == sql.ErrNoRows:
		reason := "No tenant found for this org: " + tenant.Org + " and env:" + tenant.Env
		errorCode := "UNKNOWN_SCOPE"
		return false, dbError{
			ErrorCode: errorCode,
			Reason:    reason}
	case error != nil:
		reason := error.Error()
		errorCode := "INTERNAL_SEARCH_ERROR"
		return false, dbError{
			ErrorCode: errorCode,
			Reason:    reason}
	}
	return true, dbError{}
}

// Returns developer info by querying DB directly
func getDevInfoFromDB(tenantId string, apiKey string) (developerInfo, error) {
	var apiProduct, developerApp, developerEmail sql.NullString
	var developer sql.NullString

	db := getDB()
	sSql := "SELECT ap.name, a.name, d.username, d.email " +
		"FROM kms_app_credential_apiproduct_mapper as mp " +
		"INNER JOIN kms_api_product as ap ON ap.id = mp.apiprdt_id " +
		"INNER JOIN kms_app AS a ON a.id = mp.app_id " +
		"INNER JOIN kms_developer as d ON d.id = a.developer_id " +
		"where mp.tenant_id = ? and mp.appcred_id = ?;"
	error := db.QueryRow(sSql, tenantId, apiKey).
		Scan(&apiProduct, &developerApp,
			&developer, &developerEmail)

	switch {
	case error == sql.ErrNoRows:
		log.Debugf("No data found for for tenantId = %s "+
			"and apiKey = %s in DB", tenantId, apiKey)
		return developerInfo{}, error
	case error != nil:
		log.Debugf("No data found for for tenantId = %s and "+
			"apiKey = %s due to: %v", tenantId, apiKey, error)
		return developerInfo{}, error
	}

	apiPrd := getValuesIgnoringNull(apiProduct)
	devApp := getValuesIgnoringNull(developerApp)
	dev := getValuesIgnoringNull(developer)
	devEmail := getValuesIgnoringNull(developerEmail)

	return developerInfo{ApiProduct: apiPrd,
		DeveloperApp:   devApp,
		DeveloperEmail: devEmail,
		Developer:      dev}, nil
}

// Helper method to handle scanning null values in DB to empty string
func getValuesIgnoringNull(sqlValue sql.NullString) string {
	if sqlValue.Valid {
		return sqlValue.String
	} else {
		return ""
	}
}

// Build Key as a combination of tenantId and apiKey for the developerInfo Cache
func getKeyForDeveloperInfoCache(tenantId string, apiKey string) string {
	return tenantId + "~" + apiKey
}

func getKeyForOrgEnvCache(org, env string) string {
	return org + "~" + env
}

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

// Load data scope information into an in-memory cache so that
// for each record a DB lookup is not required
func createTenantCache() {
	// Lock before writing to the map as it has multiple readers
	tenantCachelock.Lock()
	defer tenantCachelock.Unlock()
	tenantCache = make(map[string]tenant)

	var org, env, id string

	db := getDB()
	rows, error := db.Query("SELECT env, org, id FROM edgex_data_scope")

	if error != nil {
		log.Warnf("Could not get datascope from DB due to : %s", error.Error())
	} else {
		defer rows.Close()
		// Lock before writing to the map as it has multiple readers
		for rows.Next() {
			rows.Scan(&env, &org, &id)
			tenantCache[id] = tenant{Org: org, Env: env}
		}
	}

	log.Debugf("Count of data scopes in the cache: %d", len(tenantCache))
}

// Load data scope information into an in-memory cache so that
// for each record a DB lookup is not required
func createOrgEnvCache() {
	// Lock before writing to the map as it has multiple readers
	orgEnvCacheLock.Lock()
	defer orgEnvCacheLock.Unlock()
	orgEnvCache = make(map[string]bool)

	var org, env string
	db := getDB()

	rows, error := db.Query("SELECT env, org FROM edgex_data_scope")

	if error != nil {
		log.Warnf("Could not get datascope from DB due to : %s", error.Error())
	} else {
		defer rows.Close()
		// Lock before writing to the map as it has multiple readers
		for rows.Next() {
			rows.Scan(&env, &org)
			orgEnv := getKeyForOrgEnvCache(org, env)
			orgEnvCache[orgEnv] = true
		}
	}
	log.Debugf("Count of org~env in the cache: %d", len(orgEnvCache))
}

// Returns Tenant Info given a scope uuid from the cache or by querying
// the DB directly based on useCaching config
func getTenantForScope(scopeuuid string) (tenant, dbError) {
	if config.GetBool(useCaching) {
		// acquire a read lock as this cache has 1 writer as well
		tenantCachelock.RLock()
		ten, exists := tenantCache[scopeuuid]
		tenantCachelock.RUnlock()
		dbErr := dbError{}

		if !exists {
			log.Debugf("No tenant found for scopeuuid = %s "+
				"in cache, loading info from DB", scopeuuid)

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

// Returns tenant info by querying DB directly
func getTenantFromDB(scopeuuid string) (tenant, dbError) {
	var org, env string

	db := getDB()
	error := db.QueryRow("SELECT env, org FROM edgex_data_scope"+
		" where id = ?", scopeuuid).Scan(&env, &org)

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
		Org: org,
		Env: env}, dbError{}
}

/*
Checks if given org/env exists is a valid scope for this apid cluster
It also stores the scope i.e. tenant_id in the tenant object using pointer.
tenant_id in combination with apiKey is used to find kms related information
*/
func validateTenant(tenant tenant) (bool, dbError) {
	if config.GetBool(useCaching) {
		// acquire a read lock as this cache has 1 writer as well
		orgEnvCacheLock.RLock()
		orgEnv := getKeyForOrgEnvCache(tenant.Org, tenant.Env)
		_, exists := orgEnvCache[orgEnv]
		orgEnvCacheLock.RUnlock()
		dbErr := dbError{}
		if !exists {
			log.Debugf("OrgEnv = %s not found "+
				"in cache, loading info from DB", orgEnv)
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

func validateTenantFromDB(tenant tenant) (bool, dbError) {
	db := getDB()
	rows, err := db.Query("SELECT 1 FROM edgex_data_scope"+
		" where org = ? and env = ?", tenant.Org, tenant.Env)

	if !rows.Next() {
		if err == nil {
			reason := "No tenant found for this org: " + tenant.Org + " and env:" + tenant.Env
			errorCode := "UNKNOWN_SCOPE"
			return false, dbError{
				ErrorCode: errorCode,
				Reason:    reason}
		} else {
			reason := err.Error()
			errorCode := "INTERNAL_SEARCH_ERROR"
			return false, dbError{
				ErrorCode: errorCode,
				Reason:    reason}
		}
	}
	return true, dbError{}
}

func getKeyForOrgEnvCache(org, env string) string {
	return org + "~" + env
}

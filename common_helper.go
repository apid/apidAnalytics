package apidAnalytics

import (
	"database/sql"
	"fmt"
	"sync"
)

// Cache for scope uuid to org, env and tenantId information
var tenantCache map[string]tenant

// RW lock for tenant map cache since the cache can be read while its being written to and vice versa
var tenantCachelock = sync.RWMutex{}

// Cache for apiKey~tenantId to developer related information
var developerInfoCache map[string]developerInfo

// RW lock for developerInfo map cache since the cache can be read while its being written to and vice versa
var developerInfoCacheLock = sync.RWMutex{}

// Load data scope information into an in-memory cache so that for each record a DB lookup is not required
func createTenantCache() error {
	tenantCache = make(map[string]tenant)
	var org, env, tenantId, id string

	db := getDB()
	rows, error := db.Query("SELECT env, org, scope, id FROM DATA_SCOPE")

	if error != nil {
		return fmt.Errorf("Count not get datascope from DB due to: %v", error)
	} else {
		defer rows.Close()
		// Lock before writing to the map as it has multiple readers
		tenantCachelock.Lock()
		defer tenantCachelock.Unlock()
		for rows.Next() {
			rows.Scan(&env, &org, &tenantId, &id)
			tenantCache[id] = tenant{Org: org, Env: env, TenantId: tenantId}
		}
	}

	log.Debugf("Count of data scopes in the cache: %d", len(tenantCache))
	return nil
}

// Load data scope information into an in-memory cache so that for each record a DB lookup is not required
func createDeveloperInfoCache() error {
	developerInfoCache = make(map[string]developerInfo)
	var apiProduct, developerApp, developerEmail, developer sql.NullString
	var tenantId, apiKey string

	db := getDB()
	sSql := "SELECT mp.tenant_id, mp.appcred_id, ap.name, a.name, d.username, d.email " +
		"FROM APP_CREDENTIAL_APIPRODUCT_MAPPER as mp " +
		"INNER JOIN API_PRODUCT as ap ON ap.id = mp.apiprdt_id " +
		"INNER JOIN APP AS a ON a.id = mp.app_id " +
		"INNER JOIN DEVELOPER as d ON d.id = a.developer_id;"
	rows, error := db.Query(sSql)

	if error != nil {
		return fmt.Errorf("Count not get developerInfo from DB due to: %v", error)
	} else {
		defer rows.Close()
		// Lock before writing to the map as it has multiple readers
		developerInfoCacheLock.Lock()
		defer developerInfoCacheLock.Unlock()
		for rows.Next() {
			rows.Scan(&tenantId, &apiKey, &apiProduct, &developerApp, &developer, &developerEmail)

			keyForMap := getKeyForDeveloperInfoCache(tenantId, apiKey)
			apiPrd := getValuesIgnoringNull(apiProduct)
			devApp := getValuesIgnoringNull(developerApp)
			dev := getValuesIgnoringNull(developer)
			devEmail := getValuesIgnoringNull(developerEmail)

			developerInfoCache[keyForMap] = developerInfo{ApiProduct: apiPrd, DeveloperApp: devApp, DeveloperEmail: devEmail, Developer: dev}
		}
	}

	log.Debugf("Count of apiKey~tenantId combinations in the cache: %d", len(developerInfoCache))
	return nil
}

// Returns Tenant Info given a scope uuid from the cache or by querying the DB directly based on useCachig config
func getTenantForScope(scopeuuid string) (tenant, dbError) {
	if config.GetBool(useCaching) {
		_, exists := tenantCache[scopeuuid]
		if !exists {
			reason := "No tenant found for this scopeuuid: " + scopeuuid
			errorCode := "UNKNOWN_SCOPE"
			// Incase of unknown scope, try to refresh the cache ansynchronously incase an update was missed or delayed
			go createTenantCache()
			return tenant{}, dbError{ErrorCode: errorCode, Reason: reason}
		} else {
			// acquire a read lock as this cache has 1 writer as well
			tenantCachelock.RLock()
			defer tenantCachelock.RUnlock()
			return tenantCache[scopeuuid], dbError{}
		}
	} else {
		var org, env, tenantId string

		db := getDB()
		error := db.QueryRow("SELECT env, org, scope FROM DATA_SCOPE where id = ?", scopeuuid).Scan(&env, &org, &tenantId)

		switch {
		case error == sql.ErrNoRows:
			reason := "No tenant found for this scopeuuid: " + scopeuuid
			errorCode := "UNKNOWN_SCOPE"
			return tenant{}, dbError{ErrorCode: errorCode, Reason: reason}
		case error != nil:
			reason := error.Error()
			errorCode := "INTERNAL_SEARCH_ERROR"
			return tenant{}, dbError{ErrorCode: errorCode, Reason: reason}
		}
		return tenant{Org: org, Env: env, TenantId: tenantId}, dbError{}
	}
}

// Returns Dveloper related info given an apiKey and tenantId from the cache or by querying the DB directly based on useCachig config
func getDeveloperInfo(tenantId string, apiKey string) developerInfo {
	if config.GetBool(useCaching) {
		keyForMap := getKeyForDeveloperInfoCache(tenantId, apiKey)
		_, exists := developerInfoCache[keyForMap]
		if !exists {
			log.Warnf("No data found for for tenantId = %s and apiKey = %s", tenantId, apiKey)
			// Incase of unknown apiKey~tenantId, try to refresh the cache ansynchronously incase an update was missed or delayed
			go createTenantCache()
			return developerInfo{}
		} else {
			// acquire a read lock as this cache has 1 writer as well
			developerInfoCacheLock.RLock()
			defer developerInfoCacheLock.RUnlock()
			return developerInfoCache[keyForMap]
		}
	} else {
		var apiProduct, developerApp, developerEmail, developer sql.NullString

		db := getDB()
		sSql := "SELECT ap.name, a.name, d.username, d.email " +
			"FROM APP_CREDENTIAL_APIPRODUCT_MAPPER as mp " +
			"INNER JOIN API_PRODUCT as ap ON ap.id = mp.apiprdt_id " +
			"INNER JOIN APP AS a ON a.id = mp.app_id " +
			"INNER JOIN DEVELOPER as d ON d.id = a.developer_id " +
			"where mp.tenant_id = ? and mp.appcred_id = ?;"
		error := db.QueryRow(sSql, tenantId, apiKey).Scan(&apiProduct, &developerApp, &developer, &developerEmail)

		switch {
		case error == sql.ErrNoRows:
			log.Debugf("No data found for for tenantId = %s and apiKey = %s", tenantId, apiKey)
			return developerInfo{}
		case error != nil:
			log.Debugf("No data found for for tenantId = %s and apiKey = %s due to: %v", tenantId, apiKey, error)
			return developerInfo{}
		}

		apiPrd := getValuesIgnoringNull(apiProduct)
		devApp := getValuesIgnoringNull(developerApp)
		dev := getValuesIgnoringNull(developer)
		devEmail := getValuesIgnoringNull(developerEmail)
		return developerInfo{ApiProduct: apiPrd, DeveloperApp: devApp, DeveloperEmail: devEmail, Developer: dev}
	}
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

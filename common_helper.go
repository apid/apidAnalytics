package apidAnalytics

import (
	"database/sql"
	"fmt"
)

var tenantCache map[string]tenant
var developerInfoCache map[string]developerInfo

func createTenantCache() error {
	tenantCache = make(map[string]tenant)
	var org, env, tenantId, id string
	db, err := data.DB()
	if err != nil {
		return fmt.Errorf("DB not initalized")
	}

	rows, error := db.Query("SELECT env, org, scope, id FROM DATA_SCOPE")

	if error != nil {
		return fmt.Errorf("Count not get datascope from DB due to : %s", error.Error())
	} else  {
		defer rows.Close()
		for rows.Next() {
			rows.Scan(&env, &org, &tenantId, &id);
			tenantCache[id] = tenant{org: org, env: env, tenantId: tenantId}
		}
	}
	log.Debugf("Found scopes : %d", len(tenantCache))
	return nil
}

func createDeveloperInfoCache() error {
	developerInfoCache = make(map[string]developerInfo)

	var apiProduct, developerApp, developerEmail, developer  sql.NullString
	var tenantId, apiKey string

	db := getDB()

	sSql := "SELECT mp.tenant_id, mp.appcred_id, ap.name, a.name, d.username, d.email " +
		"FROM APP_CREDENTIAL_APIPRODUCT_MAPPER as mp " +
		"INNER JOIN API_PRODUCT as ap ON ap.id = mp.apiprdt_id " +
		"INNER JOIN APP AS a ON a.id = mp.app_id " +
		"INNER JOIN DEVELOPER as d ON d.id = a.developer_id ;"
	rows, error := db.Query(sSql)

	if error != nil {
		return fmt.Errorf("Count not get developerInfo from DB due to : %s", error.Error())
	} else {
		defer rows.Close()
		for rows.Next() {
			rows.Scan(&tenantId,&apiKey,&apiProduct, &developerApp, &developer, &developerEmail)

			keyForMap := getKeyForDeveloperInfoCache(tenantId, apiKey)
			apiPrd := getValuesIgnoringNull(apiProduct)
			devApp := getValuesIgnoringNull(developerApp)
			dev := getValuesIgnoringNull(developer)
			devEmail := getValuesIgnoringNull(developerEmail)

			developerInfoCache[keyForMap] = developerInfo{apiProduct: apiPrd, developerApp: devApp, developerEmail: devEmail, developer: dev}
		}
	}
	return nil
}

func getTenantForScope(scopeuuid string) (tenant, dbError) {

	if (config.GetBool(useCaching)) {
		_, exists := tenantCache[scopeuuid]
		if !exists {
			reason := "No tenant found for this scopeuuid: " + scopeuuid
			errorCode := "UNKNOWN_SCOPE"
			return tenant{}, dbError{errorCode, reason}
		} else {
			return tenantCache[scopeuuid], dbError{}
		}
	} else {
		var org, env, tenantId string
		db, err := data.DB()
		if err != nil {
			reason := "DB not initialized"
			errorCode := "INTERNAL_SEARCH_ERROR"
			return tenant{}, dbError{errorCode, reason}
		}

		error := db.QueryRow("SELECT env, org, scope FROM DATA_SCOPE where id = ?", scopeuuid).Scan(&env, &org, &tenantId)

		switch {
		case error == sql.ErrNoRows:
			reason := "No tenant found for this scopeuuid: " + scopeuuid
			errorCode := "UNKNOWN_SCOPE"
			return tenant{}, dbError{errorCode, reason}
		case error != nil:
			reason := error.Error()
			errorCode := "INTERNAL_SEARCH_ERROR"
			return tenant{}, dbError{errorCode, reason}
		}

		return tenant{org: org, env:env, tenantId: tenantId}, dbError{}
	}
}

func getDeveloperInfo(tenantId string, apiKey string) developerInfo {
	if (config.GetBool(useCaching)) {
	keyForMap := getKeyForDeveloperInfoCache(tenantId, apiKey)
		_, exists := developerInfoCache[keyForMap]
		if !exists {
			log.Debugf("No data found for for tenantId = %s and apiKey = %s", tenantId, apiKey)
			return developerInfo{}
		} else {
			return developerInfoCache[keyForMap]
		}
	} else {
		var apiProduct, developerApp, developerEmail, developer  sql.NullString

		db := getDB()
		sSql := "SELECT ap.name, a.name, d.username, d.email " +
			"FROM APP_CREDENTIAL_APIPRODUCT_MAPPER as mp " +
			"INNER JOIN API_PRODUCT as ap ON ap.id = mp.apiprdt_id " +
			"INNER JOIN APP AS a ON a.id = mp.app_id " +
			"INNER JOIN DEVELOPER as d ON d.id = a.developer_id " +
			"where mp.tenant_id = " + tenantId + " and mp.appcred_id = " + apiKey + ";"
		error := db.QueryRow(sSql).Scan(&apiProduct, &developerApp, &developer, &developerEmail)

		switch {
		case error == sql.ErrNoRows:
			log.Debug("No info found for tenantId : " + tenantId + " and apikey : " + apiKey)
			return developerInfo{}
		case error != nil:
			log.Debug("No info found for tenantId : " + tenantId + " and apikey : " + apiKey + " due to " + error.Error())
			return developerInfo{}
		}

		apiPrd := getValuesIgnoringNull(apiProduct)
		devApp := getValuesIgnoringNull(developerApp)
		dev := getValuesIgnoringNull(developer)
		devEmail := getValuesIgnoringNull(developerEmail)

		return developerInfo{apiProduct: apiPrd, developerApp: devApp, developerEmail: devEmail, developer: dev}
	}
}

func getValuesIgnoringNull(sqlValue sql.NullString) string {
	if sqlValue.Valid {
		return sqlValue.String
	} else {
		return ""
	}
}

func getKeyForDeveloperInfoCache(tenantId string, apiKey string) string {
	return tenantId + "~" + apiKey
}

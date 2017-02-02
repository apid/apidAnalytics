package apidAnalytics

import (
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
)

type handler struct{}

func (h *handler) String() string {
	return "apigeeAnalytics"
}

func (h *handler) Handle(e apid.Event) {
	snapData, ok := e.(*common.Snapshot)
	if ok {
		processSnapshot(snapData)
	} else {
		changeSet, ok := e.(*common.ChangeList)
		if ok {
			processChange(changeSet)
		} else {
			log.Errorf("Received Invalid event. Ignoring. %v", e)
		}
	}
	return
}

func processSnapshot(snapshot *common.Snapshot) {
	log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)

	db, err := data.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Unable to access database: %v", err)
	}
	setDB(db)

	if config.GetBool(useCaching) {
		err = createTenantCache()
		if err != nil {
			log.Error(err)
		} else {
			log.Debug("Created a local cache for datasope information")
		}
		err = createDeveloperInfoCache()
		if err != nil {
			log.Error(err)
		} else {
			log.Debug("Created a local cache for developer information")
		}
	} else {
		log.Info("Will not be caching any developer info and make a DB call for every analytics msg")
	}
	return
}

func processChange(changes *common.ChangeList) {
	if config.GetBool(useCaching) {
		log.Debugf("apigeeSyncEvent: %d changes", len(changes.Changes))
		var rows []common.Row

		for _, payload := range changes.Changes {
			rows = nil
			switch payload.Table {
			case "edgex.data_scope":
				switch payload.Operation {
				case common.Insert, common.Update:
					rows = append(rows, payload.NewRow)
					// Lock before writing to the map as it has multiple readers
					tenantCachelock.Lock()
					defer tenantCachelock.Unlock()
					for _, ele := range rows {
						var scopeuuid, tenantid, org, env string
						ele.Get("id", &scopeuuid)
						ele.Get("scope", &tenantid)
						ele.Get("org", &org)
						ele.Get("env", &env)
						tenantCache[scopeuuid] = tenant{Org: org, Env: env, TenantId: tenantid}
						log.Debugf("Refreshed local tenantCache. Added scope: %s", scopeuuid)
					}
				case common.Delete:
					rows = append(rows, payload.NewRow)
					// Lock before writing to the map as it has multiple readers
					tenantCachelock.Lock()
					defer tenantCachelock.Unlock()
					for _, ele := range rows {
						var scopeuuid string
						ele.Get("id", &scopeuuid)
						delete(tenantCache, scopeuuid)
						log.Debugf("Refreshed local tenantCache. Deleted scope: %s", scopeuuid)
					}
				}
			case "kms.developer", "kms.app", "kms.api_product", "kms.app_credential_apiproduct_mapper":
				// any change in any of the above tables should result in cache refresh
				createDeveloperInfoCache()
				log.Debug("Refresh local developerInfoCache")
			}
		}
	}
}

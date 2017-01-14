package apidAnalytics
import (
	"github.com/30x/apid"
	"github.com/apigee-labs/transicator/common"
)

type handler struct {
}

func (h *handler) String() string {
	return "apidAnalytics"
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
	return
}

func processChange(changes *common.ChangeList) {

	log.Debugf("apigeeSyncEvent: %d changes", len(changes.Changes))
	var rows []common.Row

	for _, payload := range changes.Changes {
		rows = nil
		switch payload.Table {
		case "edgex.data_scope":
			switch payload.Operation {
			case common.Insert, common.Update:
				rows = append(rows, payload.NewRow)
				for _, ele := range rows {
					var scopeuuid, tenantid, org, env string
					ele.Get("id", &scopeuuid)
					ele.Get("scope", &tenantid)
					ele.Get("org", &org)
					ele.Get("env", &env)
					tenantCache[scopeuuid] = tenant{org: org, env: env, tenantId: tenantid}
				}
			case common.Delete:
				rows = append(rows, payload.NewRow)
				for _, ele := range rows {
					var scopeuuid string
					ele.Get("id", &scopeuuid)
					delete(tenantCache, scopeuuid)
				}
			}
		case "kms.developer", "kms.app", "kms.api_product", "kms.app_credential_apiproduct_mapper":
			// any change in any of the above tables should result in cache refresh
			createDeveloperInfoCache()
			log.Debug("refreshed local developerInfoCache")
		}
	}
}

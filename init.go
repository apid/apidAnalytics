package apidAnalytics

import (
	"fmt"
	"github.com/30x/apid"
	"os"
	"sync"
	"path/filepath"
)

// TODO: figure out how to get these from a apid config file vs constant values
const (
	configAnalyticsBasePath  = "apidanalytics_base_path" // config
	analyticsBasePathDefault = "/analytics"

	configAnalyticsDataPath  = "apidanalytics_data_path" // config
	analyticsDataPathDefault = "/ax"

	analyticsCollectionInterval        = "apidanalytics_collection_interval" // config in seconds
	analyticsCollectionIntervalDefault = "120"

	analyticsCollectionNoFiles        = "apidanalytics_collection_no_files" // config
	analyticsCollectionNoFilesDefault = "1"

	analyticsUploadInterval        = "apidanalytics_upload_interval" // config in seconds
	analyticsUploadIntervalDefault = "5"

	uapServerBase = "apidanalytics_uap_server_base" // config

	useCaching = "apidanalytics_use_caching"
	useCachingDefault = true

	maxRetries = 3
)

// keep track of the services that this plugin will use
// note: services would also be available directly via the package global "apid" (eg. `apid.Log()`)
var (
	log    apid.LogService
	config apid.ConfigService
	data   apid.DataService
	events apid.EventsService
	unsafeDB apid.DB
	dbMux    sync.RWMutex

	localAnalyticsBaseDir      string
	localAnalyticsTempDir      string
	localAnalyticsStagingDir   string
	localAnalyticsFailedDir    string
	localAnalyticsRecoveredDir string
	uapEndpoint string
)

// apid.RegisterPlugin() is required to be called in init()
func init() {
	apid.RegisterPlugin(initPlugin)
}

func getDB() apid.DB {
	dbMux.RLock()
	db := unsafeDB
	dbMux.RUnlock()
	return db
}

func setDB(db apid.DB) {
	dbMux.Lock()
	unsafeDB = db
	dbMux.Unlock()
}

// initPlugin will be called by apid to initialize
func initPlugin(services apid.Services) (apid.PluginData, error) {

	// set a logger that is annotated for this plugin
	log = services.Log().ForModule("analytics")
	log.Debug("start init for apidAnalytics plugin")

	// set configuration
	err := setConfig(services)
	if err != nil {
		return pluginData, fmt.Errorf("Missing required config value:  %s: ", err)
	}

	for _, key := range []string{uapServerBase} {
		if !config.IsSet(key) {
			return pluginData, fmt.Errorf("Missing required config value: %s", key)
		}
	}
	uapEndpoint = uapServerBase + "/analytics"

	directories := []string{localAnalyticsBaseDir, localAnalyticsTempDir, localAnalyticsStagingDir, localAnalyticsFailedDir, localAnalyticsRecoveredDir}
	err = createDirectories(directories)

	if err != nil {
		return pluginData, fmt.Errorf("Cannot create required local directories %s: ", err)
	}

	data = services.Data()
	events = services.Events()
	events.Listen("ApigeeSync", &handler{})

	// TODO: perform crash recovery
	initUploadManager()

	if (config.GetBool(useCaching)) {
		err = createTenantCache()
		if err != nil {
			return pluginData, fmt.Errorf("Could not create tenant cache %s: ", err)
		}
		log.Debug("Created a local cache for datasope information")

		err = createDeveloperInfoCache()
		if err != nil {
			return pluginData, fmt.Errorf("Could not creata developer info cache %s: ", err)
		}
		log.Debug("Created a local cache for developer and app information")
	} else {
		log.Debug("Will not be caching any info and make a DB call for every analytics msg")
	}

	initAPI(services)

	log.Debug("end init for apidAnalytics plugin")
	return pluginData, nil
}

func setConfig(services apid.Services) error {
	config = services.Config()

	// set plugin config defaults
	config.SetDefault(configAnalyticsBasePath, analyticsBasePathDefault)
	config.SetDefault(configAnalyticsDataPath, analyticsDataPathDefault)

	if !config.IsSet("local_storage_path") {
		return fmt.Errorf("Missing required config value: local_storage_path")
	}

	// set local directory paths that will be used to buffer analytics data on disk
	localAnalyticsBaseDir = filepath.Join(config.GetString("local_storage_path"), config.GetString(configAnalyticsDataPath))
	localAnalyticsTempDir = filepath.Join(localAnalyticsBaseDir, "tmp")
	localAnalyticsStagingDir = filepath.Join(localAnalyticsBaseDir, "staging")
	localAnalyticsFailedDir = filepath.Join(localAnalyticsBaseDir, "failed")
	localAnalyticsRecoveredDir = filepath.Join(localAnalyticsBaseDir, "recovered")

	// set default config for collection interval and number of files per interval
	config.SetDefault(analyticsCollectionInterval, analyticsCollectionIntervalDefault)
	config.SetDefault(analyticsCollectionNoFiles, analyticsCollectionNoFilesDefault)

	// set default config for local caching
	config.SetDefault(useCaching, useCachingDefault)

	// set default config for upload interval
	config.SetDefault(analyticsUploadInterval, analyticsUploadIntervalDefault)

	return nil
}

// create all missing directories if required
func createDirectories(directories []string) error {
	for _, path := range directories {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			error := os.Mkdir(path, os.ModePerm)
			if error != nil {
				return error
			}
			log.Infof("created directory %s: ", path)
		}
	}
	return nil
}

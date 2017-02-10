package apidAnalytics

import (
	"fmt"
	"github.com/30x/apid-core"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// Base path of analytics API that will be exposed
	configAnalyticsBasePath  = "apidanalytics_base_path"
	analyticsBasePathDefault = "/analytics"

	// Root directory for analytics local data buffering
	configAnalyticsDataPath  = "apidanalytics_data_path"
	analyticsDataPathDefault = "/ax"

	// Data collection and buffering interval in seconds
	analyticsCollectionInterval        = "apidanalytics_collection_interval"
	analyticsCollectionIntervalDefault = "120"

	// Interval in seconds based on which staging directory
	// will be checked for folders ready to be uploaded
	analyticsUploadInterval        = "apidanalytics_upload_interval"
	analyticsUploadIntervalDefault = "5"

	// Number of slots for internal channel buffering of
	// analytics records before they are dumped to a file
	analyticsBufferChannelSize        = "apidanalytics_buffer_channel_size"
	analyticsBufferChannelSizeDefault = 1000

	// EdgeX endpoint base path to access Uap Collection Endpoint
	uapServerBase = "apidanalytics_uap_server_base"

	// If caching is used then data scope and developer
	// info will be maintained in-memory
	// cache to avoid DB calls for each analytics message
	useCaching        = "apidanalytics_use_caching"
	useCachingDefault = false

	// Interval in seconds when the developer cache should be refreshed
	analyticsCacheRefreshInterval         = "apidanalytics_cache_refresh_interval"
	analyticsCacheRefreshIntervaleDefault = 1800
)

// keep track of the services that this plugin will use
var (
	log      apid.LogService
	config   apid.ConfigService
	data     apid.DataService
	events   apid.EventsService
	unsafeDB apid.DB
	dbMux    sync.RWMutex

	localAnalyticsBaseDir      string
	localAnalyticsTempDir      string
	localAnalyticsStagingDir   string
	localAnalyticsFailedDir    string
	localAnalyticsRecoveredDir string
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
	log = services.Log().ForModule("apidAnalytics")
	log.Debug("start init for apidAnalytics plugin")

	data = services.Data()
	events = services.Events()
	events.Listen("ApigeeSync", &handler{})

	// set configuration
	err := setConfig(services)
	if err != nil {
		return pluginData, err
	}

	for _, key := range []string{uapServerBase} {
		if !config.IsSet(key) {
			return pluginData,
				fmt.Errorf("Missing required config value: %s", key)
		}
	}

	// Create directories for managing buffering and upload to UAP stages
	directories := []string{localAnalyticsBaseDir,
		localAnalyticsTempDir,
		localAnalyticsStagingDir,
		localAnalyticsFailedDir,
		localAnalyticsRecoveredDir}
	err = createDirectories(directories)

	if err != nil {
		return pluginData, fmt.Errorf("Cannot create "+
			"required local directories: %v ", err)
	}

	// Initialize one time crash recovery to be performed by the plugin on start up
	initCrashRecovery()

	// Initialize upload manager to watch the staging directory and
	// upload files to UAP as they are ready
	initUploadManager()

	// Initialize buffer manager to watch the internalBuffer channel
	// for new messages and dump them to files
	initBufferingManager()

	// Initialize developerInfo cache invalidation periodically
	if config.GetBool(useCaching) {
		updateDeveloperInfoCache()
		go func() {
			ticker := time.NewTicker(time.Second *
				config.GetDuration(analyticsCacheRefreshInterval))
			// Ticker will keep running till go routine is running
			// i.e. till application is running
			defer ticker.Stop()

			for range ticker.C {
				updateDeveloperInfoCache()
			}
		}()
	}

	// Initialize API's and expose them
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
	localAnalyticsBaseDir = filepath.Join(config.GetString("local_storage_path"),
		config.GetString(configAnalyticsDataPath))
	localAnalyticsTempDir = filepath.Join(localAnalyticsBaseDir, "tmp")
	localAnalyticsStagingDir = filepath.Join(localAnalyticsBaseDir, "staging")
	localAnalyticsFailedDir = filepath.Join(localAnalyticsBaseDir, "failed")
	localAnalyticsRecoveredDir = filepath.Join(localAnalyticsBaseDir, "recovered")

	// set default config for collection interval
	config.SetDefault(analyticsCollectionInterval, analyticsCollectionIntervalDefault)

	// set default config for useCaching
	config.SetDefault(useCaching, useCachingDefault)

	// set default config for cache refresh interval
	config.SetDefault(analyticsCacheRefreshInterval, analyticsCacheRefreshIntervaleDefault)

	// set default config for upload interval
	config.SetDefault(analyticsUploadInterval, analyticsUploadIntervalDefault)

	// set default config for internal buffer size
	config.SetDefault(analyticsBufferChannelSize, analyticsBufferChannelSizeDefault)

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
			log.Infof("created directory for analytics "+
				"data collection: %s", path)
		}
	}
	return nil
}

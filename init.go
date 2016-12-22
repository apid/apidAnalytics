package apidAnalytics

import (
	"fmt"
	"github.com/30x/apid"
	"os"
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

	uapEndpoint = "apidanalytics_uap_endpoint" // config

	uapRepo        = "apidanalytics_uap_repo" // config
	uapRepoDefault = "edge"

	uapDataset        = "apidanalytics_uap_dataset" // config
	uapDatasetDefault = "api"

	maxRetries = 3
)

// keep track of the services that this plugin will use
// note: services would also be available directly via the package global "apid" (eg. `apid.Log()`)
var (
	log    apid.LogService
	config apid.ConfigService
	data   apid.DataService

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

	for _, key := range []string{uapEndpoint} {
		if !config.IsSet(key) {
			return pluginData, fmt.Errorf("Missing required config value: %s", key)
		}
	}

	directories := []string{localAnalyticsBaseDir, localAnalyticsTempDir, localAnalyticsStagingDir, localAnalyticsFailedDir, localAnalyticsRecoveredDir}
	err = createDirectories(directories)

	if err != nil {
		return pluginData, fmt.Errorf("Cannot create required local directories %s: ", err)
	}

	data = services.Data()

	// TODO: perform crash recovery
	initUploadManager()
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

	// set default config for upload interval
	config.SetDefault(analyticsUploadInterval, analyticsUploadIntervalDefault)

	// set defaults for uap related properties
	config.SetDefault(uapRepo, uapRepoDefault)
	config.SetDefault(uapDataset, uapDatasetDefault)

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

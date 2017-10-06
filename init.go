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
	"fmt"
	"github.com/apid/apid-core"
	"os"
	"path/filepath"
	"sync"
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
	apid.RegisterPlugin(initPlugin, pluginData)
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

	// Create a listener for shutdown event and register callback
	h := func(event apid.Event) {
		log.Infof("Received ApidShutdown event. %v", event)
		shutdownPlugin()
		return
	}
	log.Infof("registered listener for shutdown event")
	events.ListenOnceFunc(apid.ShutdownEventSelector, h)

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

func shutdownPlugin() {
	log.Info("Shutting down apidAnalytics plugin")

	// close channel so new records cannot be inserted
	close(internalBuffer)
	log.Debugf("sent signal to close internal buffer channel")

	// close channel so new events for closing bucket cannot be posted
	close(closeBucketEvent)
	log.Debugf("sent signal to close closebucketevent channel")

	// block on channel to ensure channel is closed
	<-doneInternalBufferChan
	log.Debugf("closed internal buffer channel successfully")

	// block on channel to ensure channel is closed
	<-doneClosebucketChan
	log.Debugf("closed closebucketevent channel successfully")

	// Close all open files and move directories in tmp to staging.
	bucketMaplock.RLock()
	for _, bucket := range bucketMap {
		log.Infof("closing bucket '%s' as a part of shutdown", bucket.DirName)
		closeGzipFile(bucket.FileWriter)

		dirToBeClosed := filepath.Join(localAnalyticsTempDir, bucket.DirName)
		stagingPath := filepath.Join(localAnalyticsStagingDir, bucket.DirName)
		// close files in tmp folder and move directory to
		// staging to indicate its ready for upload
		err := os.Rename(dirToBeClosed, stagingPath)
		if err != nil {
			log.Errorf("Cannot move directory '%s' from"+
				" tmp to staging folder due to '%s", bucket.DirName, err)
		}
	}
	bucketMaplock.RUnlock()

	// Reset the map after all files are closed
	bucketMaplock.Lock()
	bucketMap = nil
	bucketMaplock.Unlock()
}

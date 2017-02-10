# ApidAnalytics

This is a core plugin for [apid](http://github.com/30x/apid) and is responsible for collecting analytics data for
runtime traffic from Micro and Enterprise Gateway and puplishing to Apigee.

### Configuration

| name                                  | description                       |
|---------------------------------------|-----------------------------------|
| apidanalytics_base_path               | string. default: /analytics       |
| apidanalytics_data_path               | string. default: /ax              |
| apidanalytics_collection_interval     | int. seconds. default: 120        |
| apidanalytics_upload_interval         | int. seconds. default: 5          |
| apidanalytics_uap_server_base         | string. url. required.            |
| apidanalytics_use_caching             | boolean. default: true            |
| apidanalytics_buffer_channel_size     | int. number of slots. default: 100|
| apidanalytics_cache_refresh_interval  | int. seconds. default: 1800       |

### Startup Procedure
1. Initialize crash recovery, upload and buffering manager to handle buffering analytics messages to files
   locally and then periodically upload these files to S3/GCS based on signedURL received from
   uapCollectionEndpoint exposed via edgex proxy
2. Create a listener for Apigee-Sync event
    1. Each time a Snapshot is received, create an in-memory cache for data scope
    2. Each time a changeList is received, if data_scope info changed, then insert/delete info for changed scope from tenantCache
    3. Developer info cache will be invalidated periodically and populated when 1st request for that apiKey comes in
3. Initialize POST /analytics/{scope_uuid} API
4. Upon receiving POST requests
    1. Validate and enrich each batch of analytics records
        1. If developerCache does not have info for apiKey then get from DB and insert into cache.
           This way the cache will only have info for developers/app with active traffic
    2. If valid, then publish records to an internal buffer channel
5. Buffering Logic
    1. Buffering manager creates listener on the internal buffer channel and thus consumes messages
       as soon as they are put on the channel
    2. Based on the current timestamp either an existing directory is used to save these messages
       or new a new timestamp directory is created
    3. If a new directory is created, then an event will be published on the closeBucketEvent Channel
       at the expected directory closing time
    4. The messages are stored in a file under tmp/<timestamp_directory>
    5. Based on collection interval, periodically the files in tmp are closed by the routine listening on the
       closeBucketEvent channel and the directory is moved to staging directory
6. Upload Manager
    1. The upload manager periodically checks the staging directory to look for new folders
    2. When a new folder arrives here, it means all files under that are closed and ready to uploaded
    3. Tenant info is extracted from the directory name and the files are sequentially uploaded to S3/GCS
    4. Based on the upload status
        1. If upload is successful then directory is deleted from staging and previously failed uploads are retried
        2. if upload fails, then upload is retried 3 times before moving the directory to failed directory
7. Crash Recovery is a one time activity performed when the plugin is started to
   cleanly handle open files from a previous Apid stop or crash event

### Exposed API
```sh
POST /analytics/{bundle_scope_uuid}
```
Complete spec is listed in  `api.yaml`

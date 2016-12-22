package main

import (
	"github.com/30x/apid"
	"github.com/30x/apid/factory"

	_ "github.com/30x/apidAnalytics"
)

func main() {
	// initialize apid using default services
	apid.Initialize(factory.DefaultServicesFactory())

	log := apid.Log()

	// this will call all initialization functions on all registered plugins
	apid.InitializePlugins()

	// print the base url to the console
	config := apid.Config()
	basePath := config.GetString("analyticsBasePath")
	port := config.GetString("api_port")
	log.Printf("Analytics API is at: http://localhost:%s%s", port, basePath)

	// start client API listener
	api := apid.API()
	err := api.Listen() // doesn't return if no error
	log.Fatalf("Error. Is something already running on port %d? %s", port, err)
}

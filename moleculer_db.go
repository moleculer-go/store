package db

import (
	"github.com/moleculer-go/moleculer"
	log "github.com/sirupsen/logrus"
)

func findAction(context moleculer.Context, params moleculer.Payload) interface{} {

	return nil
}

var defaultSettings = map[string]interface{}{
	"xxx": "yyy",
}

//Service create the Mixin schema for the Moleculer DB Service.
func Service() moleculer.Mixin {

	var instanceSettings = defaultSettings

	return moleculer.Mixin{
		Name:     "db-mixin",
		Settings: defaultSettings,
		Created: func(svc moleculer.Service, logger *log.Entry) {

		},
		Started: func(context moleculer.BrokerContext, svc moleculer.Service) {
			instanceSettings = svc.Settings

		},
		Stopped: func(context moleculer.BrokerContext, svc moleculer.Service) {

		},
		Actions: []moleculer.Action{
			{
				Name:    "find",
				Handler: findAction,
			},
		},
	}
}

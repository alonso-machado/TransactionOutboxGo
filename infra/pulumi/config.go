package main

import (
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi/config"
)

// stackConfig holds every value the Pulumi.<stack>.yaml files set — see
// Pulumi.dev.yaml / Pulumi.prod.yaml for the per-environment values, and
// README.md for how dbPassword/rabbitmqPassword (always secret, never
// defaulted) are expected to be configured before `pulumi up`.
type stackConfig struct {
	environment          string
	nodeInstanceType     string
	nodeDesiredCapacity  int
	nodeMinCapacity      int
	nodeMaxCapacity      int
	dbInstanceClass      string
	dbMultiAz            bool
	rabbitmqInstanceType string
	// Two separate keys, NOT one shared `imageTag` — Track 3 runs two
	// independent CI pipelines (ingestion-api.yml, consumer-worker.yml),
	// each deploying only its own service. A single shared key would mean
	// either pipeline's `pulumi up` redeploys BOTH services' pods.
	imageTagIngestionApi   string
	imageTagConsumerWorker string
	dbPassword             pulumi.StringOutput
	rabbitmqPassword       pulumi.StringOutput
}

func loadConfig(ctx *pulumi.Context) *stackConfig {
	c := config.New(ctx, "transaction-outbox")
	return &stackConfig{
		environment:            c.Get("environment"),
		nodeInstanceType:       c.Get("nodeInstanceType"),
		nodeDesiredCapacity:    c.GetInt("nodeDesiredCapacity"),
		nodeMinCapacity:        c.GetInt("nodeMinCapacity"),
		nodeMaxCapacity:        c.GetInt("nodeMaxCapacity"),
		dbInstanceClass:        c.Get("dbInstanceClass"),
		dbMultiAz:              c.GetBool("dbMultiAz"),
		rabbitmqInstanceType:   c.Get("rabbitmqInstanceType"),
		imageTagIngestionApi:   c.Get("imageTagIngestionApi"),
		imageTagConsumerWorker: c.Get("imageTagConsumerWorker"),
		dbPassword:             c.RequireSecret("dbPassword"),
		rabbitmqPassword:       c.RequireSecret("rabbitmqPassword"),
	}
}

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
	environment         string
	nodeInstanceType    string
	nodeDesiredCapacity int
	nodeMinCapacity     int
	nodeMaxCapacity     int
	dbInstanceClass      string
	dbMultiAz            bool
	rabbitmqInstanceType string
	imageTag             string
	dbPassword           pulumi.StringOutput
	rabbitmqPassword     pulumi.StringOutput
}

func loadConfig(ctx *pulumi.Context) *stackConfig {
	c := config.New(ctx, "transaction-outbox")
	return &stackConfig{
		environment:          c.Get("environment"),
		nodeInstanceType:     c.Get("nodeInstanceType"),
		nodeDesiredCapacity:  c.GetInt("nodeDesiredCapacity"),
		nodeMinCapacity:      c.GetInt("nodeMinCapacity"),
		nodeMaxCapacity:      c.GetInt("nodeMaxCapacity"),
		dbInstanceClass:      c.Get("dbInstanceClass"),
		dbMultiAz:            c.GetBool("dbMultiAz"),
		rabbitmqInstanceType: c.Get("rabbitmqInstanceType"),
		imageTag:             c.Get("imageTag"),
		dbPassword:           c.RequireSecret("dbPassword"),
		rabbitmqPassword:     c.RequireSecret("rabbitmqPassword"),
	}
}

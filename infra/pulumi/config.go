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
	environment string
	// awsRegion backs the chart's externalSecret.region value
	// (helmcharts/transaction-outbox/templates/externalsecret.yaml's
	// SecretStore — Phase 5 Track 5.A). Set this to the same region as the
	// stack's `aws:region` config (see Pulumi.dev.yaml/Pulumi.prod.yaml) —
	// kept as its own `transaction-outbox:awsRegion` key rather than reading
	// the provider's own config back out, since the Go SDK's
	// provider-config introspection varies across pulumi-aws versions.
	awsRegion string
	// drRegion (Phase 5 Track 5.C) is the destination region AWS Backup
	// copies RDS recovery points into (data.go's newBackupPlan). Left unset,
	// recovery points are still taken but never copied cross-region — see
	// the warning newBackupPlan logs in that case and docs/runbook.md.
	drRegion             string
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
	// albControllerPolicyArn is the ARN of the IAM policy granting the AWS
	// Load Balancer Controller's permissions (Phase 4 Track 1/4 — provisions
	// the ALB behind the ingestion-api Ingress / canary traffic routing).
	// Deliberately NOT authored as inline IAM JSON here: it's AWS's own
	// published, versioned policy document
	// (https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/main/docs/install/iam_policy.json),
	// several hundred lines and updated upstream as the controller gains
	// features — hand-transcribing it risks a stale or subtly wrong
	// permission set for a controller that can create/delete internet-facing
	// load balancers. Operator runs `aws iam create-policy` against that
	// document once per account and sets this config to the resulting ARN.
	albControllerPolicyArn string
}

func loadConfig(ctx *pulumi.Context) *stackConfig {
	c := config.New(ctx, "transaction-outbox")
	return &stackConfig{
		environment:            c.Get("environment"),
		awsRegion:              c.Get("awsRegion"),
		drRegion:               c.Get("drRegion"),
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
		albControllerPolicyArn: c.Get("albControllerPolicyArn"),
	}
}

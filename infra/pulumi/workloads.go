package main

import (
	"fmt"

	kubernetes "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	k8shelm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// chartPath is relative to this Pulumi project directory (infra/pulumi/),
// pointing at the existing chart rather than re-authoring workload
// manifests here — see the Track 4 plan's status note: per-method consumer
// Deployments + KEDA ScaledObjects are already templated inside the chart
// from its own paymentMethods list in values.yaml.
const chartPath = "../../helmcharts/transaction-outbox"

// installWorkloads installs helmcharts/transaction-outbox unmodified except
// for the values that only exist once the surrounding cloud resources do:
// the CI-built ECR image tags and the real Amazon MQ/RDS connection
// strings. It does not loop over payment methods — the chart already does
// that internally.
func installWorkloads(ctx *pulumi.Context, cfg *stackConfig, cluster *clusterStack, data *dataStack, provider *kubernetes.Provider) error {
	imageFor := func(repoName string) pulumi.StringOutput {
		for _, repo := range cluster.EcrRepos {
			if repo.name == repoName {
				return pulumi.Sprintf("%s:%s", repo.url, cfg.imageTag)
			}
		}
		return pulumi.String(repoName + ":" + cfg.imageTag).ToStringOutput()
	}

	_, err := k8shelm.NewRelease(ctx, "transaction-outbox", &k8shelm.ReleaseArgs{
		Name: pulumi.String("transaction-outbox"),
		Path: pulumi.String(chartPath),
		Values: pulumi.Map{
			"namespace": pulumi.String("transaction-outbox"),
			"image": pulumi.Map{
				"ingestionApi":   imageFor("ingestion-api"),
				"consumerWorker": imageFor("consumer-worker"),
			},
			"secret": pulumi.Map{
				"databaseUrl": data.databaseURL,
				"rabbitmqUrl": data.rabbitmqURL,
			},
			// Pin each microservice to its own EKS node group (cluster.go) —
			// never shared capacity between ingestion-api and consumer-worker.
			"ingestionApi": pulumi.Map{
				"nodeSelector": pulumi.Map{nodeGroupLabel: pulumi.String("ingestion-api")},
				"service": pulumi.Map{
					// The ONLY service exposed outside the cluster: an
					// internet-facing NLB. consumer-worker has no Service at
					// all, and RabbitMQ/Postgres are locked to the node
					// security group only (data.go) — nothing else is
					// reachable from outside.
					"type": pulumi.String("LoadBalancer"),
					"annotations": pulumi.Map{
						"service.beta.kubernetes.io/aws-load-balancer-type":     pulumi.String("nlb"),
						"service.beta.kubernetes.io/aws-load-balancer-internal": pulumi.String("false"),
					},
				},
			},
			"consumerWorker": pulumi.Map{
				"nodeSelector": pulumi.Map{nodeGroupLabel: pulumi.String("consumer-worker")},
			},
		},
	}, pulumi.Provider(provider))
	if err != nil {
		return fmt.Errorf("install transaction-outbox chart: %w", err)
	}
	return nil
}

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
//
// dependsOn must include the Argo Rollouts and AWS Load Balancer Controller
// releases (main.go) — this release renders Rollout/AnalysisTemplate/Ingress
// CRs that those controllers' CRDs/webhooks must already exist for.
func installWorkloads(ctx *pulumi.Context, cfg *stackConfig, cluster *clusterStack, data *dataStack, provider *kubernetes.Provider, dependsOn []pulumi.Resource) error {
	// Each service resolves its OWN tag (imageTagIngestionApi /
	// imageTagConsumerWorker — see config.go) so that one service's CI
	// pipeline deploying never drags the other service's image along.
	imageFor := func(repoName, tag string) pulumi.StringOutput {
		for _, repo := range cluster.EcrRepos {
			if repo.name == repoName {
				return pulumi.Sprintf("%s:%s", repo.url, tag)
			}
		}
		return pulumi.String(repoName + ":" + tag).ToStringOutput()
	}

	_, err := k8shelm.NewRelease(ctx, "transaction-outbox", &k8shelm.ReleaseArgs{
		Name:  pulumi.String("transaction-outbox"),
		Chart: pulumi.String(chartPath),
		Values: pulumi.Map{
			"namespace": pulumi.String("transaction-outbox"),
			"image": pulumi.Map{
				"ingestionApi":   imageFor("ingestion-api", cfg.imageTagIngestionApi),
				"consumerWorker": imageFor("consumer-worker", cfg.imageTagConsumerWorker),
			},
			"secret": pulumi.Map{
				"databaseUrl": data.databaseURL,
				"rabbitmqUrl": data.rabbitmqURL,
			},
			// Phase 4 Track 4: Argo Rollouts controller is installed
			// (main.go) — render Rollout/AnalysisTemplate instead of plain
			// Deployment/HPA.
			"canary": pulumi.Map{
				"enabled": pulumi.Bool(true),
			},
			// Pin each microservice to its own EKS node group (cluster.go) —
			// never shared capacity between ingestion-api and consumer-worker.
			"ingestionApi": pulumi.Map{
				"nodeSelector": pulumi.Map{nodeGroupLabel: pulumi.String("ingestion-api")},
				// ClusterIP (the chart's default) — Phase 4 Track 1 replaces
				// the Phase 3 NLB with an ALB Ingress below as the front
				// door, since an NLB can neither expose the real client IP
				// (no X-Forwarded-For) nor be a WAF/canary traffic-routing
				// attach point. The AWS Load Balancer Controller (main.go)
				// targets pods directly (target-type: ip) from the public
				// subnets — same exposure topology as the old NLB,
				// ingestion-api remains the only publicly reachable service.
				"ingress": pulumi.Map{
					"enabled": pulumi.Bool(true),
				},
			},
			"consumerWorker": pulumi.Map{
				"nodeSelector": pulumi.Map{nodeGroupLabel: pulumi.String("consumer-worker")},
			},
		},
	}, pulumi.Provider(provider), pulumi.DependsOn(dependsOn))
	if err != nil {
		return fmt.Errorf("install transaction-outbox chart: %w", err)
	}
	return nil
}

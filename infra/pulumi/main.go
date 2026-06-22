// Pulumi program for the Transaction Outbox Go monorepo's AWS target
// (Track 4 of Phase 3): VPC → EKS → ECR/RDS/Amazon MQ/Secrets Manager →
// KEDA operator → the existing helmcharts/transaction-outbox chart.
//
// This program does not author Kubernetes workload manifests — the chart
// already renders one consumer Deployment + KEDA ScaledObject per payment
// method from its own values.yaml (see workloads.go).
package main

import (
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

func main() {
	pulumi.Run(func(ctx *pulumi.Context) error {
		cfg := loadConfig(ctx)

		net, err := newNetwork(ctx, cfg)
		if err != nil {
			return err
		}

		cluster, err := newCluster(ctx, cfg, net)
		if err != nil {
			return err
		}

		data, err := newData(ctx, cfg, net, cluster)
		if err != nil {
			return err
		}

		k8sProvider, err := newK8sProvider(ctx, cluster)
		if err != nil {
			return err
		}

		kedaRelease, err := installKeda(ctx, k8sProvider)
		if err != nil {
			return err
		}

		// Phase 4 Track 4: Argo Rollouts controller + (Track 1) the AWS Load
		// Balancer Controller must both be installed — and their CRDs/
		// webhooks ready — before the app chart applies any Rollout,
		// AnalysisTemplate, or ALB-annotated Ingress resources.
		argoRollouts, err := installArgoRollouts(ctx, k8sProvider)
		if err != nil {
			return err
		}

		albController, err := installAlbController(ctx, cfg, net, cluster, k8sProvider)
		if err != nil {
			return err
		}

		// Phase 5 Track 5.A: External Secrets Operator must be installed
		// (and its CRDs ready) before the app chart renders any
		// ExternalSecret resource (workloads.go sets externalSecrets.enabled
		// true below).
		esoRelease, _, err := installExternalSecretsOperator(ctx, cluster, k8sProvider)
		if err != nil {
			return err
		}

		workloadDeps := []pulumi.Resource{kedaRelease, argoRollouts, albController, esoRelease}
		if err := installWorkloads(ctx, cfg, cluster, data, k8sProvider, workloadDeps); err != nil {
			return err
		}

		ctx.Export("kubeconfig", cluster.Kubeconfig)
		ctx.Export("clusterName", cluster.EksCluster.Name())
		ctx.Export("dbEndpoint", data.dbEndpoint)
		ctx.Export("rabbitmqEndpoint", data.rabbitmqEndpoint)
		for _, repo := range cluster.EcrRepos {
			ctx.Export("ecrRepo_"+repo.name, repo.url)
		}
		return nil
	})
}

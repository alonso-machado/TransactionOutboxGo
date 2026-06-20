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

		if err := installKeda(ctx, k8sProvider); err != nil {
			return err
		}

		if err := installWorkloads(ctx, cfg, cluster, data, k8sProvider); err != nil {
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

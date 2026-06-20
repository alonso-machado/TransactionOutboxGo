package main

import (
	"fmt"

	kubernetes "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	k8shelm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// newK8sProvider builds the single Kubernetes provider both installKeda and
// installWorkloads install their Helm releases through, pointed at the EKS
// cluster's own kubeconfig.
func newK8sProvider(ctx *pulumi.Context, cluster *clusterStack) (*kubernetes.Provider, error) {
	return kubernetes.NewProvider(ctx, "k8s", &kubernetes.ProviderArgs{
		Kubeconfig: cluster.KubeconfigJson,
	})
}

// installKeda installs the upstream KEDA operator (the autoscaler engine
// that watches the ScaledObjects rendered by the app's own Helm chart — see
// workloads.go) via its official Helm chart. This is a different chart from
// helmcharts/transaction-outbox: KEDA is cluster-wide infrastructure, the
// app chart is the workload.
func installKeda(ctx *pulumi.Context, provider *kubernetes.Provider) error {
	_, err := k8shelm.NewRelease(ctx, "keda", &k8shelm.ReleaseArgs{
		Chart:           pulumi.String("keda"),
		Version:         pulumi.String("2.16.0"),
		Namespace:       pulumi.String("keda"),
		CreateNamespace: pulumi.Bool(true),
		RepositoryOpts: &k8shelm.RepositoryOptsArgs{
			Repo: pulumi.String("https://kedacore.github.io/charts"),
		},
	}, pulumi.Provider(provider))
	if err != nil {
		return fmt.Errorf("install keda chart: %w", err)
	}
	return nil
}

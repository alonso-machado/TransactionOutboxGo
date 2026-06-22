package main

import (
	"fmt"

	kubernetes "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	k8shelm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// installArgoRollouts installs the Argo Rollouts controller (Phase 4 Track 4)
// via its official Helm chart — cluster-wide infrastructure providing the
// Rollout/AnalysisTemplate CRDs that helmcharts/transaction-outbox renders
// when installed with canary.enabled=true (see workloads.go). Same pattern as
// installKeda: a separate chart from the app chart, installed first so its
// CRDs exist before the app chart's Rollout/AnalysisTemplate resources apply.
func installArgoRollouts(ctx *pulumi.Context, provider *kubernetes.Provider) (*k8shelm.Release, error) {
	release, err := k8shelm.NewRelease(ctx, "argo-rollouts", &k8shelm.ReleaseArgs{
		Chart:           pulumi.String("argo-rollouts"),
		Version:         pulumi.String("2.38.0"),
		Namespace:       pulumi.String("argo-rollouts"),
		CreateNamespace: pulumi.Bool(true),
		RepositoryOpts: &k8shelm.RepositoryOptsArgs{
			Repo: pulumi.String("https://argoproj.github.io/argo-helm"),
		},
	}, pulumi.Provider(provider))
	if err != nil {
		return nil, fmt.Errorf("install argo-rollouts chart: %w", err)
	}
	return release, nil
}

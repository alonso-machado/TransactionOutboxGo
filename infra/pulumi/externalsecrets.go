package main

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/iam"
	kubernetes "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	k8shelm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// esoNamespace and esoServiceAccount are the well-known names the upstream
// external-secrets Helm chart creates by default — the IRSA trust policy
// below must reference exactly these, since they're what the operator's pod
// actually runs as.
const (
	esoNamespace      = "external-secrets"
	esoServiceAccount = "external-secrets"
)

// installExternalSecretsOperator installs the upstream External Secrets
// Operator (ESO) via its official Helm chart — same pattern as installKeda
// (keda.go): cluster-wide infrastructure, a different chart from
// helmcharts/transaction-outbox.
//
// ESO syncs secrets from AWS Secrets Manager (the transaction-outbox/<env>/
// database-url and .../rabbitmq-url entries data.go already creates) into
// plain Kubernetes Secrets via ExternalSecret CRDs rendered by the app chart
// (helmcharts/transaction-outbox/templates/externalsecret.yaml, gated by
// externalSecrets.enabled — see workloads.go). ESO authenticates to AWS via
// IRSA (no static AWS credentials anywhere in the cluster), the same
// pattern installAlbController (albcontroller.go) uses for the AWS Load
// Balancer Controller.
func installExternalSecretsOperator(ctx *pulumi.Context, cluster *clusterStack, provider *kubernetes.Provider) (*k8shelm.Release, *iam.Role, error) {
	oidcProvider := cluster.Core.OidcProvider()

	trustPolicy := pulumi.All(oidcProvider.Arn(), oidcProvider.Url()).ApplyT(func(args []interface{}) (string, error) {
		arn := args[0].(string)
		url := args[1].(string)
		return fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Principal": {"Federated": "%s"},
				"Action": "sts:AssumeRoleWithWebIdentity",
				"Condition": {
					"StringEquals": {
						"%s:sub": "system:serviceaccount:%s:%s",
						"%s:aud": "sts.amazonaws.com"
					}
				}
			}]
		}`, arn, url, esoNamespace, esoServiceAccount, url), nil
	}).(pulumi.StringOutput)

	role, err := iam.NewRole(ctx, "external-secrets-operator", &iam.RoleArgs{
		AssumeRolePolicy: trustPolicy,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create external-secrets-operator iam role: %w", err)
	}

	// Scoped read-only access to exactly this app's Secrets Manager entries
	// (transaction-outbox/<env>/*), never a blanket secretsmanager:* grant.
	policy, err := iam.NewRolePolicy(ctx, "external-secrets-operator-secretsmanager", &iam.RolePolicyArgs{
		Role: role.Name,
		Policy: pulumi.String(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
				"Resource": "arn:aws:secretsmanager:*:*:secret:transaction-outbox/*"
			}]
		}`),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("attach external-secrets-operator secretsmanager policy: %w", err)
	}

	release, err := k8shelm.NewRelease(ctx, "external-secrets", &k8shelm.ReleaseArgs{
		Chart:           pulumi.String("external-secrets"),
		Version:         pulumi.String("0.10.4"),
		Namespace:       pulumi.String(esoNamespace),
		CreateNamespace: pulumi.Bool(true),
		RepositoryOpts: &k8shelm.RepositoryOptsArgs{
			Repo: pulumi.String("https://charts.external-secrets.io"),
		},
		Values: pulumi.Map{
			"serviceAccount": pulumi.Map{
				"create": pulumi.Bool(true),
				"name":   pulumi.String(esoServiceAccount),
				"annotations": pulumi.Map{
					"eks.amazonaws.com/role-arn": role.Arn,
				},
			},
		},
	}, pulumi.Provider(provider), pulumi.DependsOn([]pulumi.Resource{role, policy}))
	if err != nil {
		return nil, nil, fmt.Errorf("install external-secrets chart: %w", err)
	}
	return release, role, nil
}

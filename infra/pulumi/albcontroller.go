package main

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/iam"
	kubernetes "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	k8shelm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// installAlbController installs the AWS Load Balancer Controller (Phase 4
// Track 1's NLB->ALB front-door change, and Track 4's prerequisite for
// request-weighted canary traffic splitting) via its official Helm chart.
// The controller watches Ingress objects annotated `kubernetes.io/ingress.class:
// alb` (helmcharts/transaction-outbox/templates/ingestion-api/ingress*.yaml)
// and provisions/manages the actual ALB + target groups.
//
// Authenticates via IRSA: an IAM role trusted by the cluster's OIDC provider
// (cluster.go's CreateOidcProvider), assumable only by the
// kube-system/aws-load-balancer-controller service account. The IAM policy
// itself is attached by ARN (cfg.albControllerPolicyArn) rather than authored
// here — see config.go's comment on why that document isn't hand-transcribed.
func installAlbController(ctx *pulumi.Context, cfg *stackConfig, net *network, cluster *clusterStack, provider *kubernetes.Provider) (*k8shelm.Release, error) {
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
						"%s:sub": "system:serviceaccount:kube-system:aws-load-balancer-controller",
						"%s:aud": "sts.amazonaws.com"
					}
				}
			}]
		}`, arn, url, url), nil
	}).(pulumi.StringOutput)

	role, err := iam.NewRole(ctx, "aws-load-balancer-controller", &iam.RoleArgs{
		AssumeRolePolicy: trustPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("create alb controller iam role: %w", err)
	}

	if cfg.albControllerPolicyArn != "" {
		if _, err := iam.NewRolePolicyAttachment(ctx, "alb-controller-policy", &iam.RolePolicyAttachmentArgs{
			Role:      role.Name,
			PolicyArn: pulumi.String(cfg.albControllerPolicyArn),
		}); err != nil {
			return nil, fmt.Errorf("attach alb controller policy: %w", err)
		}
	} else {
		ctx.Log.Warn("albControllerPolicyArn is unset — the AWS Load Balancer Controller will install but its service account will have no AWS permissions and Ingress reconciliation will fail. Set transaction-outbox:albControllerPolicyArn once the IAM policy from https://kubernetes-sigs.github.io/aws-load-balancer-controller/latest/install/iam_policy.json has been created.", nil)
	}

	clusterName := cluster.EksCluster.Name()

	release, err := k8shelm.NewRelease(ctx, "aws-load-balancer-controller", &k8shelm.ReleaseArgs{
		Chart:           pulumi.String("aws-load-balancer-controller"),
		Version:         pulumi.String("1.8.1"),
		Namespace:       pulumi.String("kube-system"),
		CreateNamespace: pulumi.Bool(false),
		RepositoryOpts: &k8shelm.RepositoryOptsArgs{
			Repo: pulumi.String("https://aws.github.io/eks-charts"),
		},
		Values: pulumi.Map{
			"clusterName": clusterName,
			"vpcId":       net.vpc.VpcId,
			"serviceAccount": pulumi.Map{
				"create": pulumi.Bool(true),
				"name":   pulumi.String("aws-load-balancer-controller"),
				"annotations": pulumi.Map{
					"eks.amazonaws.com/role-arn": role.Arn,
				},
			},
		},
	}, pulumi.Provider(provider), pulumi.DependsOn([]pulumi.Resource{role}))
	if err != nil {
		return nil, fmt.Errorf("install aws-load-balancer-controller chart: %w", err)
	}
	return release, nil
}

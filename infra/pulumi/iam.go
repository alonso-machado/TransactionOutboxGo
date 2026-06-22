package main

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/iam"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// nodeIAMRole creates the single IAM role shared by every EKS worker node
// group (ingestion-api's and consumer-worker's alike) — the per-group
// isolation the user asked for is expressed at the node-group/network layer
// (separate ManagedNodeGroups, separate node-only security group ingress
// for RDS/Amazon MQ), not via separate IAM identities, which would add no
// extra isolation here since neither workload calls AWS APIs directly.
func nodeIAMRole(ctx *pulumi.Context) (*iam.Role, error) {
	role, err := iam.NewRole(ctx, "transaction-outbox-node-role", &iam.RoleArgs{
		AssumeRolePolicy: pulumi.String(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Action": "sts:AssumeRole",
				"Effect": "Allow",
				"Principal": {"Service": "ec2.amazonaws.com"}
			}]
		}`),
	})
	if err != nil {
		return nil, fmt.Errorf("create node iam role: %w", err)
	}

	for _, policyArn := range []string{
		"arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
		"arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
		"arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
	} {
		if _, err := iam.NewRolePolicyAttachment(ctx, "node-role-"+policyName(policyArn), &iam.RolePolicyAttachmentArgs{
			Role:      role.Name,
			PolicyArn: pulumi.String(policyArn),
		}); err != nil {
			return nil, fmt.Errorf("attach policy %s: %w", policyArn, err)
		}
	}
	return role, nil
}

func policyName(arn string) string {
	for i := len(arn) - 1; i >= 0; i-- {
		if arn[i] == '/' {
			return arn[i+1:]
		}
	}
	return arn
}

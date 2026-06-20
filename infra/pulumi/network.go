package main

import (
	"github.com/pulumi/pulumi-awsx/sdk/go/awsx/ec2"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// network is the VPC + public/private subnets + NAT every other resource
// (EKS, RDS, Amazon MQ) is provisioned into.
type network struct {
	vpc *ec2.Vpc
}

// newNetwork provisions a 2-AZ VPC with public + private subnets and a NAT
// gateway, via the higher-level awsx Vpc component (per the Track 4 plan's
// "pulumi-eks/awsx higher-level component for brevity" decision) rather than
// hand-rolled aws.ec2 resources.
func newNetwork(ctx *pulumi.Context, cfg *stackConfig) (*network, error) {
	twoAZs := 2
	vpc, err := ec2.NewVpc(ctx, "transaction-outbox", &ec2.VpcArgs{
		NumberOfAvailabilityZones: &twoAZs,
		// NatGateways left unset: with both public and private subnets
		// present, awsx defaults to one NAT Gateway per AZ — a documented
		// awsx default, not a hand-picked strategy.
		Tags: pulumi.StringMap{
			"Environment": pulumi.String(cfg.environment),
			"Project":     pulumi.String("transaction-outbox"),
		},
	})
	if err != nil {
		return nil, err
	}
	return &network{vpc: vpc}, nil
}

package main

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/ecr"
	awseks "github.com/pulumi/pulumi-aws/sdk/v5/go/aws/eks"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/iam"
	"github.com/pulumi/pulumi-eks/sdk/go/eks"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

// ecrRepo bundles an ECR repository's logical name with its resolved
// pull/push URL, exported by main.go and consumed by workloads.go to build
// the chart's image.ingestionApi/image.consumerWorker values.
type ecrRepo struct {
	name string
	url  pulumi.StringOutput
}

// clusterStack is the EKS cluster, its two per-service node groups, and the
// ECR repositories CI pushes SHA-tagged images to.
type clusterStack struct {
	*eks.Cluster
	EcrRepos []ecrRepo
}

// services is the set of binaries built by build/Dockerfile (ARG SERVICE)
// and pushed to ECR by the CI docker job (Track 3).
var services = []string{"ingestion-api", "consumer-worker"}

// nodeGroupLabel is the Kubernetes node label each ManagedNodeGroup below
// carries; the chart's ingestionApi.nodeSelector/consumerWorker.nodeSelector
// values (set in workloads.go) target this label so each service's pods
// only ever land on that service's own node group — never shared capacity.
const nodeGroupLabel = "workload"

// newCluster provisions the EKS control plane with no default node group,
// then two purpose-built ManagedNodeGroups — one per microservice, each its
// own (Graviton/arm64) instance type and Kubernetes node label, per the
// "ingestion-api and consumer-worker are different microservices, give them
// their own nodes" requirement. Both groups run in private subnets only
// (NodeAssociatePublicIpAddress: false): internet exposure for
// ingestion-api happens at the Service/LoadBalancer layer (workloads.go),
// never via a public node IP, and consumer-worker is never reachable from
// outside the cluster at all.
func newCluster(ctx *pulumi.Context, cfg *stackConfig, net *network) (*clusterStack, error) {
	nodeRole, err := nodeIAMRole(ctx)
	if err != nil {
		return nil, err
	}

	cluster, err := eks.NewCluster(ctx, "transaction-outbox", &eks.ClusterArgs{
		VpcId:                       net.vpc.VpcId,
		PublicSubnetIds:             net.vpc.PublicSubnetIds,
		PrivateSubnetIds:            net.vpc.PrivateSubnetIds,
		NodeAssociatePublicIpAddress: pulumi.BoolRef(false),
		SkipDefaultNodeGroup: pulumi.BoolRef(true),
		InstanceRoles:        iam.RoleArray{nodeRole},
		// Phase 4 Track 1/4: the AWS Load Balancer Controller's service
		// account authenticates via IRSA (albcontroller.go), which needs the
		// cluster's own OIDC provider to exist.
		CreateOidcProvider: pulumi.BoolPtr(true),
	})
	if err != nil {
		return nil, fmt.Errorf("create eks cluster: %w", err)
	}

	if _, err := eks.NewManagedNodeGroup(ctx, "ingestion-api", &eks.ManagedNodeGroupArgs{
		Cluster:       cluster,
		NodeRole:      nodeRole,
		AmiType:       pulumi.String("AL2_ARM_64"), // Graviton
		InstanceTypes: pulumi.StringArray{pulumi.String(cfg.nodeInstanceType)},
		SubnetIds:     net.vpc.PrivateSubnetIds,
		Labels: pulumi.StringMap{
			nodeGroupLabel: pulumi.String("ingestion-api"),
		},
		ScalingConfig: awseks.NodeGroupScalingConfigArgs{
			DesiredSize: pulumi.Int(cfg.nodeDesiredCapacity),
			MinSize:     pulumi.Int(cfg.nodeMinCapacity),
			MaxSize:     pulumi.Int(cfg.nodeMaxCapacity),
		},
	}); err != nil {
		return nil, fmt.Errorf("create ingestion-api node group: %w", err)
	}

	if _, err := eks.NewManagedNodeGroup(ctx, "consumer-worker", &eks.ManagedNodeGroupArgs{
		Cluster:       cluster,
		NodeRole:      nodeRole,
		AmiType:       pulumi.String("AL2_ARM_64"), // Graviton — consumer-worker is event-driven, no need for x86
		InstanceTypes: pulumi.StringArray{pulumi.String(cfg.nodeInstanceType)},
		SubnetIds:     net.vpc.PrivateSubnetIds,
		Labels: pulumi.StringMap{
			nodeGroupLabel: pulumi.String("consumer-worker"),
		},
		ScalingConfig: awseks.NodeGroupScalingConfigArgs{
			// KEDA scales the consumer Deployments themselves 0→10 per
			// method (the chart's own ScaledObjects); this is cluster
			// autoscaling for the *nodes* those pods land on, kept modest
			// since the workload is lightweight per replica.
			DesiredSize: pulumi.Int(cfg.nodeMinCapacity),
			MinSize:     pulumi.Int(cfg.nodeMinCapacity),
			MaxSize:     pulumi.Int(cfg.nodeMaxCapacity),
		},
	}); err != nil {
		return nil, fmt.Errorf("create consumer-worker node group: %w", err)
	}

	repos := make([]ecrRepo, 0, len(services))
	for _, svc := range services {
		repo, err := ecr.NewRepository(ctx, svc, &ecr.RepositoryArgs{
			Name:               pulumi.String(svc),
			ImageTagMutability: pulumi.String("IMMUTABLE"), // CI tags by git SHA, never reuses a tag
			ImageScanningConfiguration: &ecr.RepositoryImageScanningConfigurationArgs{
				ScanOnPush: pulumi.Bool(true),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("create ecr repo %s: %w", svc, err)
		}
		repos = append(repos, ecrRepo{name: svc, url: repo.RepositoryUrl})
	}

	return &clusterStack{Cluster: cluster, EcrRepos: repos}, nil
}

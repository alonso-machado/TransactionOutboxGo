package main

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/go/aws/ec2"
	"github.com/pulumi/pulumi-aws/sdk/go/aws/mq"
	"github.com/pulumi/pulumi-aws/sdk/go/aws/rds"
	"github.com/pulumi/pulumi-aws/sdk/go/aws/secretsmanager"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

const (
	dbName      = "outbox"
	dbUsername  = "outbox"
	rmqUsername = "outbox"
)

// dataStack is RDS Postgres + Amazon MQ for RabbitMQ + the Secrets Manager
// entries holding their connection strings — what workloads.go injects into
// the chart's secret.databaseUrl/secret.rabbitmqUrl values.
type dataStack struct {
	dbEndpoint       pulumi.StringOutput
	rabbitmqEndpoint pulumi.StringOutput
	databaseURL      pulumi.StringOutput
	rabbitmqURL      pulumi.StringOutput
}

// newData provisions RDS PostgreSQL 17 and Amazon MQ for RabbitMQ, both
// with PubliclyAccessible=false and a dedicated security group whose only
// ingress rule sources from the EKS node security group — neither resource
// is reachable from outside the cluster's own worker nodes, let alone the
// internet. Amazon MQ (not SQS) is used specifically so the existing
// AMQP-based adapter/messaging + infrastructure/rabbitmq code needs zero
// rewrite (see the Track 4 plan's "Amazon MQ vs SQS" decision).
func newData(ctx *pulumi.Context, cfg *stackConfig, net *network, cluster *clusterStack) (*dataStack, error) {
	dataSG, err := ec2.NewSecurityGroup(ctx, "transaction-outbox-data", &ec2.SecurityGroupArgs{
		VpcId: net.vpc.VpcId,
		Ingress: ec2.SecurityGroupIngressArray{
			&ec2.SecurityGroupIngressArgs{
				Description:     pulumi.String("PostgreSQL from EKS worker nodes only"),
				Protocol:        pulumi.String("tcp"),
				FromPort:        pulumi.Int(5432),
				ToPort:          pulumi.Int(5432),
				SecurityGroups:  pulumi.StringArray{cluster.NodeSecurityGroup.ID()},
			},
			&ec2.SecurityGroupIngressArgs{
				Description:    pulumi.String("Amazon MQ AMQPS from EKS worker nodes only"),
				Protocol:       pulumi.String("tcp"),
				FromPort:       pulumi.Int(5671),
				ToPort:         pulumi.Int(5671),
				SecurityGroups: pulumi.StringArray{cluster.NodeSecurityGroup.ID()},
			},
			&ec2.SecurityGroupIngressArgs{
				Description:    pulumi.String("Amazon MQ management HTTPS (KEDA rabbitmq trigger) from EKS worker nodes only"),
				Protocol:       pulumi.String("tcp"),
				FromPort:       pulumi.Int(15671),
				ToPort:         pulumi.Int(15671),
				SecurityGroups: pulumi.StringArray{cluster.NodeSecurityGroup.ID()},
			},
		},
		Egress: ec2.SecurityGroupEgressArray{
			&ec2.SecurityGroupEgressArgs{
				Protocol:   pulumi.String("-1"),
				FromPort:   pulumi.Int(0),
				ToPort:     pulumi.Int(0),
				CidrBlocks: pulumi.StringArray{pulumi.String("0.0.0.0/0")},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create data security group: %w", err)
	}

	dbSubnetGroup, err := rds.NewSubnetGroup(ctx, "transaction-outbox-db", &rds.SubnetGroupArgs{
		SubnetIds: net.vpc.PrivateSubnetIds,
	})
	if err != nil {
		return nil, fmt.Errorf("create db subnet group: %w", err)
	}

	db, err := rds.NewInstance(ctx, "transaction-outbox-db", &rds.InstanceArgs{
		Engine:              pulumi.String("postgres"),
		EngineVersion:       pulumi.String("17"),
		InstanceClass:       pulumi.String(cfg.dbInstanceClass),
		AllocatedStorage:    pulumi.Int(20),
		DbName:              pulumi.String(dbName),
		Username:            pulumi.String(dbUsername),
		Password:            cfg.dbPassword,
		MultiAz:             pulumi.Bool(cfg.dbMultiAz),
		SkipFinalSnapshot:   pulumi.Bool(cfg.environment != "prod"),
		PubliclyAccessible:  pulumi.Bool(false),
		DbSubnetGroupName:   dbSubnetGroup.Name,
		VpcSecurityGroupIds: pulumi.StringArray{dataSG.ID()},
	})
	if err != nil {
		return nil, fmt.Errorf("create rds instance: %w", err)
	}

	broker, err := mq.NewBroker(ctx, "transaction-outbox-rabbitmq", &mq.BrokerArgs{
		BrokerName:         pulumi.String("transaction-outbox"),
		EngineType:         pulumi.String("RabbitMQ"),
		EngineVersion:      pulumi.String("3.13"),
		HostInstanceType:   pulumi.String(cfg.rabbitmqInstanceType),
		DeploymentMode:     pulumi.String("SINGLE_INSTANCE"), // CLUSTER_MULTI_AZ is the prod-grade upgrade, not built now
		PubliclyAccessible: pulumi.Bool(false),
		SubnetIds:          pulumi.StringArray{net.vpc.PrivateSubnetIds.Index(pulumi.Int(0))},
		SecurityGroups:     pulumi.StringArray{dataSG.ID()},
		Users: mq.BrokerUserArray{
			&mq.BrokerUserArgs{
				Username: pulumi.String(rmqUsername),
				Password: cfg.rabbitmqPassword,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create amazon mq broker: %w", err)
	}

	dbEndpoint := db.Address
	databaseURL := pulumi.Sprintf("postgres://%s:%s@%s/%s?sslmode=require", dbUsername, cfg.dbPassword, dbEndpoint, dbName)

	// SINGLE_INSTANCE mode exposes exactly one AMQP endpoint. The management
	// HTTPS endpoint KEDA's rabbitmq trigger needs is broker.Instances[0]'s
	// console URL — see README.md for wiring it into
	// RABBITMQ_MANAGEMENT_URL once the broker exists (it can't be derived
	// statically before apply).
	rabbitmqEndpoint := broker.Instances.Index(pulumi.Int(0)).Endpoints().Index(pulumi.Int(0))
	rabbitmqURL := pulumi.Sprintf("amqps://%s:%s@%s", rmqUsername, cfg.rabbitmqPassword, rabbitmqEndpoint)

	if _, err := secretsmanager.NewSecret(ctx, "transaction-outbox-db-url", &secretsmanager.SecretArgs{
		Name: pulumi.String(fmt.Sprintf("transaction-outbox/%s/database-url", cfg.environment)),
	}); err != nil {
		return nil, fmt.Errorf("create db secret: %w", err)
	}
	if _, err := secretsmanager.NewSecret(ctx, "transaction-outbox-rabbitmq-url", &secretsmanager.SecretArgs{
		Name: pulumi.String(fmt.Sprintf("transaction-outbox/%s/rabbitmq-url", cfg.environment)),
	}); err != nil {
		return nil, fmt.Errorf("create rabbitmq secret: %w", err)
	}

	return &dataStack{
		dbEndpoint:       dbEndpoint,
		rabbitmqEndpoint: rabbitmqURL,
		databaseURL:      databaseURL,
		rabbitmqURL:      rabbitmqURL,
	}, nil
}

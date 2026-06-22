package main

import (
	"fmt"

	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/backup"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/ec2"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/iam"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/kms"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/mq"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/rds"
	"github.com/pulumi/pulumi-aws/sdk/v5/go/aws/secretsmanager"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

const (
	dbName      = "outbox"
	dbUsername  = "outbox"
	rmqUsername = "outbox"
)

// dataStack is RDS Postgres + Amazon MQ for RabbitMQ + the Secrets Manager
// entries holding their connection strings — what workloads.go injects into
// the chart's secret.databaseUrl/secret.rabbitmqUrl values (the local/static
// Secret fallback) and what the chart's ExternalSecret CRs
// (externalsecret.yaml, Phase 5 Track 5.A) sync from when
// externalSecrets.enabled is true. dbSecretName/rabbitmqSecretName are the
// exact Secrets Manager entry names ESO's ExternalSecret.spec.dataFrom
// references.
type dataStack struct {
	dbEndpoint         pulumi.StringOutput
	rabbitmqEndpoint   pulumi.StringOutput
	databaseURL        pulumi.StringOutput
	rabbitmqURL        pulumi.StringOutput
	dbSecretName       string
	rabbitmqSecretName string
}

// newData provisions RDS PostgreSQL 18 and Amazon MQ for RabbitMQ, both
// with PubliclyAccessible=false and a dedicated security group whose only
// ingress rule sources from the EKS node security group — neither resource
// is reachable from outside the cluster's own worker nodes, let alone the
// internet. Amazon MQ (not SQS) is used specifically so the existing
// AMQP-based adapter/messaging + infrastructure/rabbitmq code needs zero
// rewrite (see the Track 4 plan's "Amazon MQ vs SQS" decision).
func newData(ctx *pulumi.Context, cfg *stackConfig, net *network, cluster *clusterStack) (*dataStack, error) {
	// cluster.NodeSecurityGroup is an ec2.SecurityGroupOutput (a pulumi-eks
	// component sub-output), which has no .ID() of its own — only the
	// underlying *ec2.SecurityGroup resource does. ApplyT flattens the
	// Output-returning callback into a single StringOutput. See
	// https://github.com/pulumi/pulumi-eks/issues/644 (a known gap in the
	// pulumi-eks Go SDK: there's no direct way to get a usable ID string off
	// this field).
	nodeSGID := cluster.NodeSecurityGroup.ApplyT(func(sg *ec2.SecurityGroup) pulumi.StringOutput {
		return sg.ID().ToStringOutput()
	}).(pulumi.StringOutput)

	dataSG, err := ec2.NewSecurityGroup(ctx, "transaction-outbox-data", &ec2.SecurityGroupArgs{
		VpcId: net.vpc.VpcId,
		Ingress: ec2.SecurityGroupIngressArray{
			&ec2.SecurityGroupIngressArgs{
				Description:    pulumi.String("PostgreSQL from EKS worker nodes only"),
				Protocol:       pulumi.String("tcp"),
				FromPort:       pulumi.Int(5432),
				ToPort:         pulumi.Int(5432),
				SecurityGroups: pulumi.StringArray{nodeSGID},
			},
			&ec2.SecurityGroupIngressArgs{
				Description:    pulumi.String("Amazon MQ AMQPS from EKS worker nodes only"),
				Protocol:       pulumi.String("tcp"),
				FromPort:       pulumi.Int(5671),
				ToPort:         pulumi.Int(5671),
				SecurityGroups: pulumi.StringArray{nodeSGID},
			},
			&ec2.SecurityGroupIngressArgs{
				Description:    pulumi.String("Amazon MQ management HTTPS (KEDA rabbitmq trigger) from EKS worker nodes only"),
				Protocol:       pulumi.String("tcp"),
				FromPort:       pulumi.Int(15671),
				ToPort:         pulumi.Int(15671),
				SecurityGroups: pulumi.StringArray{nodeSGID},
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

	// Encryption at rest (Phase 5 Track 5.B/5.C) — a dedicated CMK rather
	// than the default aws/rds key, so key ownership/rotation is explicit
	// and auditable (see SECURITY.md). EnableKeyRotation lets AWS rotate the
	// backing key material yearly without re-encrypting data (RDS handles
	// this transparently).
	dbKMSKey, err := kms.NewKey(ctx, "transaction-outbox-db", &kms.KeyArgs{
		Description:          pulumi.String("Transaction Outbox RDS encryption at rest"),
		EnableKeyRotation:    pulumi.Bool(true),
		DeletionWindowInDays: pulumi.Int(30),
	})
	if err != nil {
		return nil, fmt.Errorf("create db kms key: %w", err)
	}

	db, err := rds.NewInstance(ctx, "transaction-outbox-db", &rds.InstanceArgs{
		Engine:            pulumi.String("postgres"),
		EngineVersion:     pulumi.String("18"),
		InstanceClass:     pulumi.String(cfg.dbInstanceClass),
		AllocatedStorage:  pulumi.Int(20),
		DbName:            pulumi.String(dbName),
		Username:          pulumi.String(dbUsername),
		Password:          cfg.dbPassword,
		MultiAz:           pulumi.Bool(cfg.dbMultiAz),
		SkipFinalSnapshot: pulumi.Bool(cfg.environment != "prod"),
		// Encryption at rest via the dedicated CMK above (Track 5.B/5.C).
		StorageEncrypted: pulumi.Bool(true),
		KmsKeyId:         dbKMSKey.Arn,
		// Automated backups + PITR (Track 5.C): BackupRetentionPeriod > 0 is
		// what turns on both RDS automated daily snapshots AND continuous
		// transaction-log-based point-in-time-restore for any second within
		// the retention window — there's no separate "enable PITR" flag,
		// it's implied by a non-zero retention period. 7 days locally/dev-
		// shaped default; bump per-environment via Pulumi config if a
		// longer RPO window is wanted. BackupWindow is UTC, chosen for low
		// traffic (this is a demo, not a real production traffic profile).
		BackupRetentionPeriod: pulumi.Int(7),
		BackupWindow:          pulumi.String("03:00-04:00"),
		// Snapshots taken during this window are exactly what backup.go's
		// AWS Backup plan (below) copies cross-region — RDS's own native
		// snapshot copy is also available (rds.SnapshotCopy) but AWS Backup
		// gives a single, auditable, schedule-driven policy across both RDS
		// and (if added later) other resource types, which is the better
		// fit for a documented DR story (see docs/runbook.md).
		PubliclyAccessible:  pulumi.Bool(false),
		DbSubnetGroupName:   dbSubnetGroup.Name,
		VpcSecurityGroupIds: pulumi.StringArray{dataSG.ID()},
	})
	if err != nil {
		return nil, fmt.Errorf("create rds instance: %w", err)
	}

	if err := newBackupPlan(ctx, cfg, db); err != nil {
		return nil, err
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

	dbSecretName := fmt.Sprintf("transaction-outbox/%s/database-url", cfg.environment)
	dbSecret, err := secretsmanager.NewSecret(ctx, "transaction-outbox-db-url", &secretsmanager.SecretArgs{
		Name: pulumi.String(dbSecretName),
	})
	if err != nil {
		return nil, fmt.Errorf("create db secret: %w", err)
	}
	// Populate the secret's actual value — Track 5.A's ExternalSecret CRs
	// (helmcharts/transaction-outbox/templates/externalsecret.yaml) sync
	// from this entry into the cluster's DATABASE_URL; an empty secret would
	// make externalSecrets.enabled=true sync nothing.
	if _, err := secretsmanager.NewSecretVersion(ctx, "transaction-outbox-db-url", &secretsmanager.SecretVersionArgs{
		SecretId:     dbSecret.ID(),
		SecretString: databaseURL,
	}); err != nil {
		return nil, fmt.Errorf("create db secret version: %w", err)
	}

	rabbitmqSecretName := fmt.Sprintf("transaction-outbox/%s/rabbitmq-url", cfg.environment)
	rmqSecret, err := secretsmanager.NewSecret(ctx, "transaction-outbox-rabbitmq-url", &secretsmanager.SecretArgs{
		Name: pulumi.String(rabbitmqSecretName),
	})
	if err != nil {
		return nil, fmt.Errorf("create rabbitmq secret: %w", err)
	}
	if _, err := secretsmanager.NewSecretVersion(ctx, "transaction-outbox-rabbitmq-url", &secretsmanager.SecretVersionArgs{
		SecretId:     rmqSecret.ID(),
		SecretString: rabbitmqURL,
	}); err != nil {
		return nil, fmt.Errorf("create rabbitmq secret version: %w", err)
	}

	return &dataStack{
		dbEndpoint:         dbEndpoint,
		rabbitmqEndpoint:   rabbitmqURL,
		databaseURL:        databaseURL,
		rabbitmqURL:        rabbitmqURL,
		dbSecretName:       dbSecretName,
		rabbitmqSecretName: rabbitmqSecretName,
	}, nil
}

// newBackupPlan wires AWS Backup around the RDS instance: a daily backup
// rule that copies each recovery point into cfg.drRegion (Phase 5 Track
// 5.C's cross-region snapshot copy). AWS Backup, not RDS's own
// rds.SnapshotCopy, is used so the DR story is one auditable, schedule-
// driven policy (visible in the AWS Backup console/API as a single plan)
// rather than a per-snapshot copy call — see docs/runbook.md for the
// restore procedure this plan exists to support.
//
// If cfg.drRegion is unset, the plan still runs (same-region recovery
// points, useful on its own) but logs a warning that cross-region copy is
// disabled — mirrors the albControllerPolicyArn "degrade, don't fail"
// pattern in albcontroller.go.
func newBackupPlan(ctx *pulumi.Context, cfg *stackConfig, db *rds.Instance) error {
	vault, err := backup.NewVault(ctx, "transaction-outbox-db", &backup.VaultArgs{})
	if err != nil {
		return fmt.Errorf("create backup vault: %w", err)
	}

	role, err := iam.NewRole(ctx, "transaction-outbox-backup", &iam.RoleArgs{
		AssumeRolePolicy: pulumi.String(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Action": "sts:AssumeRole",
				"Effect": "Allow",
				"Principal": {"Service": "backup.amazonaws.com"}
			}]
		}`),
	})
	if err != nil {
		return fmt.Errorf("create backup iam role: %w", err)
	}
	for _, policyArn := range []string{
		"arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForBackup",
		"arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForRestores",
	} {
		if _, err := iam.NewRolePolicyAttachment(ctx, "backup-role-"+policyName(policyArn), &iam.RolePolicyAttachmentArgs{
			Role:      role.Name,
			PolicyArn: pulumi.String(policyArn),
		}); err != nil {
			return fmt.Errorf("attach backup policy %s: %w", policyArn, err)
		}
	}

	rule := &backup.PlanRuleArgs{
		RuleName:        pulumi.String("daily"),
		TargetVaultName: vault.Name,
		// 03:30 UTC — just after the RDS BackupWindow above, so the AWS
		// Backup recovery point and the RDS automated snapshot don't
		// contend for I/O at the same minute.
		Schedule: pulumi.String("cron(30 3 * * ? *)"),
		Lifecycle: &backup.PlanRuleLifecycleArgs{
			DeleteAfter: pulumi.Int(35), // days — slightly past the RDS 7-day PITR window, see runbook RPO/RTO notes
		},
	}
	if cfg.drRegion != "" {
		rule.CopyActions = backup.PlanRuleCopyActionArray{
			&backup.PlanRuleCopyActionArgs{
				DestinationVaultArn: pulumi.Sprintf("arn:aws:backup:%s:*:backup-vault:%s", cfg.drRegion, "transaction-outbox-db-dr"),
				Lifecycle: &backup.PlanRuleCopyActionLifecycleArgs{
					DeleteAfter: pulumi.Int(35),
				},
			},
		}
	} else {
		ctx.Log.Warn("transaction-outbox:drRegion is unset — RDS recovery points are backed up but never copied cross-region. Set it (e.g. us-west-2) to enable the Track 5.C DR posture.", nil)
	}

	plan, err := backup.NewPlan(ctx, "transaction-outbox-db", &backup.PlanArgs{
		Rules: backup.PlanRuleArray{rule},
	})
	if err != nil {
		return fmt.Errorf("create backup plan: %w", err)
	}

	if _, err := backup.NewSelection(ctx, "transaction-outbox-db", &backup.SelectionArgs{
		IamRoleArn: role.Arn,
		PlanId:     plan.ID(),
		Resources:  pulumi.StringArray{db.Arn},
	}); err != nil {
		return fmt.Errorf("create backup selection: %w", err)
	}
	return nil
}

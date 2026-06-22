// Phase 5 Track 4 — cloud counterpart of the local docker-compose
// observability stack (Loki/Tempo/Alertmanager/alert rules). Same pattern as
// keda.go/argorollouts.go: each piece of cluster-wide observability
// infrastructure is its own Helm release, installed via the shared
// Kubernetes provider (newK8sProvider in keda.go).
//
// DEVIATION FROM THE PLAN: the plan only asks for Tempo/Loki Helm releases
// plus PrometheusRule CRDs + Alertmanager config "following whatever k8s
// resource pattern the file already uses" — but no existing Pulumi file in
// this repo installs kube-prometheus-stack (Prometheus/Grafana/Alertmanager
// Operator) at all; Phase 4's Grafana+Prometheus only exist in
// docker-compose.yml for local dev. PrometheusRule is a CRD that
// kube-prometheus-stack's Prometheus Operator provides — without installing
// that operator there is no CRD for a PrometheusRule resource to target. So
// this file also installs kube-prometheus-stack itself (Prometheus, the
// Operator/CRDs, Grafana, Alertmanager) as the minimum viable prerequisite,
// clearly called out here rather than silently assumed.
package main

import (
	"fmt"

	kubernetes "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes"
	apiextensions "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/apiextensions"
	corev1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/core/v1"
	metav1 "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/meta/v1"
	k8shelm "github.com/pulumi/pulumi-kubernetes/sdk/v4/go/kubernetes/helm/v3"
	"github.com/pulumi/pulumi/sdk/v3/go/pulumi"
)

const observabilityNamespace = "observability"

// installKubePrometheusStack installs the Prometheus Operator + Prometheus +
// Grafana + Alertmanager via the upstream kube-prometheus-stack chart — the
// cloud equivalent of docker-compose.yml's prometheus/grafana/alertmanager
// services, and the prerequisite for the PrometheusRule CRD that
// installPrometheusRules (below) depends on.
func installKubePrometheusStack(ctx *pulumi.Context, alertmanagerSecret *corev1.Secret, provider *kubernetes.Provider) (*k8shelm.Release, error) {
	release, err := k8shelm.NewRelease(ctx, "kube-prometheus-stack", &k8shelm.ReleaseArgs{
		Chart:           pulumi.String("kube-prometheus-stack"),
		Version:         pulumi.String("65.5.0"),
		Namespace:       pulumi.String(observabilityNamespace),
		CreateNamespace: pulumi.Bool(true),
		RepositoryOpts: &k8shelm.RepositoryOptsArgs{
			Repo: pulumi.String("https://prometheus-community.github.io/helm-charts"),
		},
		Values: pulumi.Map{
			// Same single-secret-driven Alertmanager config convention as the
			// local observability/alertmanager/alertmanager.yml file —
			// alertmanagerSecret carries the equivalent YAML in-cluster.
			"alertmanager": pulumi.Map{
				"alertmanagerSpec": pulumi.Map{
					"alertmanagerConfigSecret": alertmanagerSecret.Metadata.Name(),
				},
			},
			// Let PrometheusRule resources outside the chart's own release
			// namespace/labels still get picked up — installPrometheusRules
			// below applies them as their own resources, not via this chart's
			// values.
			"prometheus": pulumi.Map{
				"prometheusSpec": pulumi.Map{
					"ruleSelectorNilUsesHelmValues":           pulumi.Bool(false),
					"serviceMonitorSelectorNilUsesHelmValues": pulumi.Bool(false),
				},
			},
		},
	}, pulumi.Provider(provider), pulumi.DependsOn([]pulumi.Resource{alertmanagerSecret}))
	if err != nil {
		return nil, fmt.Errorf("install kube-prometheus-stack chart: %w", err)
	}
	return release, nil
}

// installTempo installs Grafana Tempo (Phase 5 Track 4.B) via its official
// Helm chart — the cloud counterpart of docker-compose.yml's tempo service,
// replacing Jaeger so traces correlate natively with the kube-prometheus-stack
// Grafana installed above.
func installTempo(ctx *pulumi.Context, provider *kubernetes.Provider) (*k8shelm.Release, error) {
	release, err := k8shelm.NewRelease(ctx, "tempo", &k8shelm.ReleaseArgs{
		Chart:           pulumi.String("tempo"),
		Version:         pulumi.String("1.10.1"),
		Namespace:       pulumi.String(observabilityNamespace),
		CreateNamespace: pulumi.Bool(true),
		RepositoryOpts: &k8shelm.RepositoryOptsArgs{
			Repo: pulumi.String("https://grafana.github.io/helm-charts"),
		},
		Values: pulumi.Map{
			"tempo": pulumi.Map{
				"storage": pulumi.Map{
					"trace": pulumi.Map{
						// Local-disk backend, same as observability/tempo/tempo.yaml —
						// swap for the chart's s3 backend config once an S3 bucket
						// for trace storage is provisioned (out of Track 4 scope).
						"backend": pulumi.String("local"),
					},
				},
			},
		},
	}, pulumi.Provider(provider))
	if err != nil {
		return nil, fmt.Errorf("install tempo chart: %w", err)
	}
	return release, nil
}

// installLoki installs Grafana Loki in single-binary mode (Phase 5 Track
// 4.A) — the cloud counterpart of docker-compose.yml's loki + promtail
// services. The chart's own Promtail/Alloy sub-chart (enabled by default)
// handles log shipping in-cluster, so no separate DaemonSet install is
// needed here.
func installLoki(ctx *pulumi.Context, provider *kubernetes.Provider) (*k8shelm.Release, error) {
	release, err := k8shelm.NewRelease(ctx, "loki", &k8shelm.ReleaseArgs{
		Chart:           pulumi.String("loki"),
		Version:         pulumi.String("6.20.0"),
		Namespace:       pulumi.String(observabilityNamespace),
		CreateNamespace: pulumi.Bool(true),
		RepositoryOpts: &k8shelm.RepositoryOptsArgs{
			Repo: pulumi.String("https://grafana.github.io/helm-charts"),
		},
		Values: pulumi.Map{
			"loki": pulumi.Map{
				"commonConfig": pulumi.Map{
					"replication_factor": pulumi.Int(1),
				},
				"storage": pulumi.Map{
					"type": pulumi.String("filesystem"),
				},
			},
			// Single-binary mode for the showcase deployment — same shape as
			// the local docker-compose loki service. Swap for the chart's
			// "distributed" deployment mode + an S3-backed storage config at
			// real production scale (out of Track 4 scope).
			"deploymentMode": pulumi.String("SingleBinary"),
			"singleBinary": pulumi.Map{
				"replicas": pulumi.Int(1),
			},
		},
	}, pulumi.Provider(provider))
	if err != nil {
		return nil, fmt.Errorf("install loki chart: %w", err)
	}
	return release, nil
}

// installAlertmanagerSecret creates the Kubernetes Secret carrying
// alertmanager.yml's content — kube-prometheus-stack's documented way of
// supplying Alertmanager config when not using its AlertmanagerConfig CRD.
// Mirrors observability/alertmanager/alertmanager.yml exactly (same
// single-route, single-receiver, documented-as-placeholder webhook).
func installAlertmanagerSecret(ctx *pulumi.Context, provider *kubernetes.Provider) (*corev1.Secret, error) {
	// alertmanager.yaml is the literal key name kube-prometheus-stack's
	// Alertmanager StatefulSet expects inside this secret.
	const alertmanagerYAML = `route:
  receiver: default
  group_by: ["alertname"]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  routes:
    - matchers:
        - severity = "page"
      group_wait: 10s
      repeat_interval: 1h
      receiver: default
receivers:
  - name: default
    webhook_configs:
      # PLACEHOLDER — replace with your real Slack/PagerDuty/email endpoint
      # before relying on this outside a demo cluster.
      - url: "http://localhost:9999/replace-with-your-real-webhook"
        send_resolved: true
`

	secret, err := corev1.NewSecret(ctx, "alertmanager-config", &corev1.SecretArgs{
		Metadata: &metav1.ObjectMetaArgs{
			Name:      pulumi.String("alertmanager-transaction-outbox"),
			Namespace: pulumi.String(observabilityNamespace),
		},
		StringData: pulumi.StringMap{
			"alertmanager.yaml": pulumi.String(alertmanagerYAML),
		},
	}, pulumi.Provider(provider))
	if err != nil {
		return nil, fmt.Errorf("create alertmanager config secret: %w", err)
	}
	return secret, nil
}

// installPrometheusRules applies one PrometheusRule CRD mirroring
// observability/prometheus/rules/alerts.yml's groups verbatim, so the same
// alert definitions fire whether evaluated by the local docker-compose
// Prometheus or by kube-prometheus-stack's Prometheus in EKS — Phase 4's
// existing single-source-of-truth convention for dashboards extended to
// alert rules.
func installPrometheusRules(ctx *pulumi.Context, dependsOn []pulumi.Resource, provider *kubernetes.Provider) (*apiextensions.CustomResource, error) {
	rule, err := apiextensions.NewCustomResource(ctx, "transaction-outbox-alerts", &apiextensions.CustomResourceArgs{
		ApiVersion: pulumi.String("monitoring.coreos.com/v1"),
		Kind:       pulumi.String("PrometheusRule"),
		Metadata: &metav1.ObjectMetaArgs{
			Name:      pulumi.String("transaction-outbox-alerts"),
			Namespace: pulumi.String(observabilityNamespace),
			Labels: pulumi.StringMap{
				// kube-prometheus-stack's default Prometheus CR only picks up
				// PrometheusRule resources carrying this label (matches the
				// chart's ruleSelector default) — set explicitly since
				// ruleSelectorNilUsesHelmValues=false above only relaxes the
				// namespace restriction, not the label selector itself.
				"release": pulumi.String("kube-prometheus-stack"),
			},
		},
		OtherFields: map[string]interface{}{
			"spec": map[string]interface{}{
				"groups": []map[string]interface{}{
					{
						"name": "transaction-outbox.outbox",
						"rules": []map[string]interface{}{
							{
								"alert":  "OutboxBacklogGrowing",
								"expr":   "deriv(outbox_pending_count[5m]) > 0",
								"for":    "5m",
								"labels": map[string]interface{}{"severity": "ticket"},
								"annotations": map[string]interface{}{
									"summary":     "Outbox pending count has been rising for 5+ minutes",
									"description": "outbox.pending_count has had a positive derivative for at least 5 minutes — the dispatcher is falling behind RabbitMQ/DB pressure.",
								},
							},
							{
								"alert":  "OutboxDeadLetterPresent",
								"expr":   "outbox_dead_letter_count > 0",
								"for":    "1m",
								"labels": map[string]interface{}{"severity": "page"},
								"annotations": map[string]interface{}{
									"summary":     "Outbox has dead-lettered messages",
									"description": "outbox.dead_letter_count > 0 — at least one outbox message exhausted OUTBOX_MAX_RETRIES and was marked DEAD_LETTER. Requires manual replay (cmd/outbox-admin's replay-dead command).",
								},
							},
						},
					},
					{
						"name": "transaction-outbox.rabbitmq",
						"rules": []map[string]interface{}{
							{
								"alert":  "RabbitMQDLQDepth",
								"expr":   `rabbitmq_queue_messages{queue=~"payments\\..*\\.dlq"} > 0`,
								"for":    "1m",
								"labels": map[string]interface{}{"severity": "page"},
								"annotations": map[string]interface{}{
									"summary":     "A payments DLQ has messages",
									"description": "A payments.<method>.dlq queue is non-empty — messages were rejected after MAX_DELIVERIES attempts.",
								},
							},
						},
					},
					{
						"name": "transaction-outbox.consumer",
						"rules": []map[string]interface{}{
							{
								"alert":  "ConsumerPoisonRate",
								"expr":   `sum(rate(consumer_messages_processed_total{outcome="poison_dlq"}[5m])) by (payment_method) > 0`,
								"for":    "2m",
								"labels": map[string]interface{}{"severity": "page"},
								"annotations": map[string]interface{}{
									"summary":     "Consumer is poison-dead-lettering messages",
									"description": "consumer.messages_processed_total{outcome=\"poison_dlq\"} rate > 0 — messages are exhausting MAX_DELIVERIES.",
								},
							},
							{
								"alert":  "UnknownSchemaVersion",
								"expr":   "consumer_unknown_schema_version_total > 0",
								"for":    "1m",
								"labels": map[string]interface{}{"severity": "ticket"},
								"annotations": map[string]interface{}{
									"summary":     "Consumer received a message with an unknown schema version",
									"description": "consumer.unknown_schema_version_total > 0 — a message's envelope schemaVersion wasn't recognized by this consumer build. Check for a producer/consumer version skew during a rollout.",
								},
							},
						},
					},
					{
						"name": "transaction-outbox.ratelimit",
						"rules": []map[string]interface{}{
							{
								"alert":  "RateLimitRejectSpike",
								"expr":   "sum(rate(ingestion_ratelimit_rejected_total[5m])) > 5",
								"for":    "5m",
								"labels": map[string]interface{}{"severity": "ticket"},
								"annotations": map[string]interface{}{
									"summary":     "ingestion-api is rejecting a sustained volume of requests on the rate limiter",
									"description": "ingestion.ratelimit_rejected_total rate has exceeded 5/s for 5+ minutes.",
								},
							},
						},
					},
					{
						"name": "transaction-outbox.pgbouncer",
						"rules": []map[string]interface{}{
							{
								"alert":  "PgBouncerClientsWaiting",
								"expr":   "pgbouncer_pools_client_waiting_connections > 0",
								"for":    "2m",
								"labels": map[string]interface{}{"severity": "ticket"},
								"annotations": map[string]interface{}{
									"summary":     "PgBouncer clients are waiting for a server connection",
									"description": "pgbouncer_pools_client_waiting_connections > 0 for 2+ minutes — the pool is too small for the current KEDA fan-out.",
								},
							},
						},
					},
				},
			},
		},
	}, pulumi.Provider(provider), pulumi.DependsOn(dependsOn))
	if err != nil {
		return nil, fmt.Errorf("apply transaction-outbox-alerts PrometheusRule: %w", err)
	}
	return rule, nil
}

// installObservability wires up the full Phase 5 Track 4 cloud observability
// stack and returns the resources installWorkloads (or main.go) may want to
// depend on / wait for. Mirrors installKeda/installArgoRollouts's pattern:
// one function per concern, composed from main.go.
func installObservability(ctx *pulumi.Context, provider *kubernetes.Provider) ([]pulumi.Resource, error) {
	alertmanagerSecret, err := installAlertmanagerSecret(ctx, provider)
	if err != nil {
		return nil, err
	}

	kubePromStack, err := installKubePrometheusStack(ctx, alertmanagerSecret, provider)
	if err != nil {
		return nil, err
	}

	tempo, err := installTempo(ctx, provider)
	if err != nil {
		return nil, err
	}

	loki, err := installLoki(ctx, provider)
	if err != nil {
		return nil, err
	}

	// The PrometheusRule CRD only exists once kube-prometheus-stack's
	// Prometheus Operator has installed its CRDs.
	rules, err := installPrometheusRules(ctx, []pulumi.Resource{kubePromStack}, provider)
	if err != nil {
		return nil, err
	}

	return []pulumi.Resource{kubePromStack, tempo, loki, rules}, nil
}

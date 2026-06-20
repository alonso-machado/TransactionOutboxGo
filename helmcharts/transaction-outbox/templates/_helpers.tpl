{{/*
Standard app.kubernetes.io labels, merged into every resource's metadata.labels
alongside the resource-specific "app: <name>" selector label.
*/}}
{{- define "transaction-outbox.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
=======================================================================
  aragog-exporters — Helm Template Helpers
  Naming convention aligned with helm-charts.* reference pattern
=======================================================================
*/}}

{{/*
Expand the name of the chart.
*/}}
{{- define "helm-charts.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fullname: release-chart (truncated to 63 chars for K8s).
*/}}
{{- define "helm-charts.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "helm-charts.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "helm-charts.labels" -}}
helm.sh/chart: {{ include "helm-charts.chart" . }}
{{ include "helm-charts.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (subset of common labels for matchLabels).
*/}}
{{- define "helm-charts.selectorLabels" -}}
app.kubernetes.io/name: {{ include "helm-charts.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Labels for a specific exporter deployment.
  Usage: {{ include "helm-charts.exporterLabels" (dict "root" . "exporterName" $name "exporterType" $type) }}
*/}}
{{- define "helm-charts.exporterLabels" -}}
{{ include "helm-charts.labels" .root }}
aragog.io/component: exporter
aragog.io/exporter: {{ .exporterName }}
aragog.io/type: {{ .exporterType }}
{{- end }}

{{/*
Selector labels for a specific exporter.
*/}}
{{- define "helm-charts.exporterSelectorLabels" -}}
{{ include "helm-charts.selectorLabels" .root }}
aragog.io/component: exporter
aragog.io/exporter: {{ .exporterName }}
{{- end }}

{{/*
Labels for writer deployments.
  Usage: {{ include "helm-charts.writerLabels" (dict "root" . "writerName" "pg") }}
*/}}
{{- define "helm-charts.writerLabels" -}}
{{ include "helm-charts.labels" .root }}
aragog.io/component: writer
aragog.io/writer: {{ .writerName }}
{{- end }}

{{/*
Selector labels for writer deployments.
*/}}
{{- define "helm-charts.writerSelectorLabels" -}}
{{ include "helm-charts.selectorLabels" .root }}
aragog.io/component: writer
aragog.io/writer: {{ .writerName }}
{{- end }}

{{/*
Secret name used across all resources (kept for backward compat).
*/}}
{{- define "helm-charts.secretName" -}}
{{ include "helm-charts.fullname" . }}-secrets
{{- end }}

{{/*
Resolve per-exporter resources with fallback to exporterDefaults.
*/}}
{{- define "helm-charts.exporterResources" -}}
{{- $res := .exporter.resources | default .defaults.resources }}
requests:
  cpu: {{ $res.requests.cpu | quote }}
  memory: {{ $res.requests.memory | quote }}
limits:
  cpu: {{ $res.limits.cpu | quote }}
  memory: {{ $res.limits.memory | quote }}
{{- end }}

{{/*
Kafka topic for exporter based on type.
*/}}
{{- define "helm-charts.kafkaTopic" -}}
{{- if eq .type "organization" -}}
{{ .topics.orgsValidated }}
{{- else -}}
{{ .topics.reviewsValidated }}
{{- end -}}
{{- end }}

{{/*
Vault annotations for pod template (from reference project pattern).
*/}}
{{- define "helm-charts.vaultAnnotations" -}}
vault.hashicorp.com/agent-inject: "true"
vault.hashicorp.com/role: {{ .Values.vault.role | quote }}
vault.hashicorp.com/agent-inject-secret-env: {{ .Values.vault.secretPath | quote }}
vault.hashicorp.com/tls-skip-verify: "true"
vault.hashicorp.com/agent-inject-template-env: |
  {{`{{- with secret `}}{{ .Values.vault.secretPath | quote }}{{` -}}`}}
  {{`{{ range $k, $v := .Data.data }}`}}
  export {{`{{ $k }}`}}="{{`{{ $v }}`}}"
  {{`{{ end }}`}}
  {{`{{- end }}`}}
{{- end }}

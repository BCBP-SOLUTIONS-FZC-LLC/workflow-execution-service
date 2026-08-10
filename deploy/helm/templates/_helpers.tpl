{{/*
Base name for the chart / release.
*/}}
{{- define "workflow-execution-service.name" -}}
{{- .Values.nameOverride | default .Chart.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Full release name, e.g. "my-release-workflow-execution-service".
*/}}
{{- define "workflow-execution-service.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := .Values.nameOverride | default .Chart.Name }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Per-process fullname, e.g. "<fullname>-api" / "<fullname>-worker".
*/}}
{{- define "workflow-execution-service.apiFullname" -}}
{{- printf "%s-api" (include "workflow-execution-service.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "workflow-execution-service.workerFullname" -}}
{{- printf "%s-worker" (include "workflow-execution-service.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "workflow-execution-service.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels, shared by every resource regardless of process.
*/}}
{{- define "workflow-execution-service.labels" -}}
helm.sh/chart: {{ include "workflow-execution-service.chart" . }}
{{ include "workflow-execution-service.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "workflow-execution-service.selectorLabels" -}}
app.kubernetes.io/name: {{ include "workflow-execution-service.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Per-process selector labels — used by both the Deployment's own pod
template labels and its Service/PDB/HPA selector, so each process only ever
matches its own pods.
*/}}
{{- define "workflow-execution-service.apiSelectorLabels" -}}
{{ include "workflow-execution-service.selectorLabels" . }}
app.kubernetes.io/component: api
{{- end }}

{{- define "workflow-execution-service.workerSelectorLabels" -}}
{{ include "workflow-execution-service.selectorLabels" . }}
app.kubernetes.io/component: worker
{{- end }}

{{/*
Name of the Secret holding secret-backed env vars — either an
operator-provided existingSecretName, or this release's own.
*/}}
{{- define "workflow-execution-service.secretName" -}}
{{- .Values.secret.existingSecretName | default (include "workflow-execution-service.fullname" .) }}
{{- end }}

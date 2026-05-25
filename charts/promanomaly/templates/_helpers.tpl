{{/* Expand the name of the chart. */}}
{{- define "promanomaly.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Create a default fully qualified app name. */}}
{{- define "promanomaly.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Chart label string. */}}
{{- define "promanomaly.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Common labels applied to every resource. */}}
{{- define "promanomaly.labels" -}}
helm.sh/chart: {{ include "promanomaly.chart" . }}
{{ include "promanomaly.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels — must be stable across upgrades. */}}
{{- define "promanomaly.selectorLabels" -}}
app.kubernetes.io/name: {{ include "promanomaly.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* ServiceAccount name to use. */}}
{{- define "promanomaly.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "promanomaly.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* ConfigMap name: either the user-supplied existing CM or the one we render. */}}
{{- define "promanomaly.configMapName" -}}
{{- if .Values.existingConfigMap -}}
{{- .Values.existingConfigMap -}}
{{- else -}}
{{- printf "%s-config" (include "promanomaly.fullname" .) -}}
{{- end -}}
{{- end -}}

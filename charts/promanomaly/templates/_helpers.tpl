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

{{/*
  Render the body of one sink (the fields below ``type:``) at column 0;
  the caller indents the result for the scalar ``sink:`` mapping or each
  ``sinks:`` list item. Shared by configmap.yaml so the two forms can
  never drift. Input dict: ``sink`` (the sink values) and ``tokenPath``
  (where the bearer token Secret is mounted, when one is set).

  Optional fields (``timeout``, ``auth``, ``max_annotations_per_run``,
  ``tags``) are emitted only when set: ``sinks:`` list entries are
  user-supplied and may omit them, and the detector's config schema fills
  the documented defaults. Only the typed block and its ``url`` are
  required (the config schema enforces a block matching ``type``).
*/}}
{{- define "promanomaly.sinkFields" -}}
{{- $sink := .sink -}}
{{- $tokenPath := .tokenPath -}}
type: {{ $sink.type | quote }}
{{- if eq $sink.type "remote_write" }}
{{- $rw := $sink.remote_write }}
remote_write:
  url: {{ $rw.url | quote }}
  {{- with $rw.timeout }}
  timeout: {{ . | quote }}
  {{- end }}
  {{- with $rw.auth }}
  auth:
    type: {{ .type | default "none" | quote }}
    {{- if .existingSecret }}
    token_file: {{ $tokenPath }}
    {{- end }}
  {{- end }}
{{- else if eq $sink.type "grafana_annotations" }}
{{- $ga := $sink.grafana_annotations }}
grafana_annotations:
  url: {{ $ga.url | quote }}
  {{- with $ga.timeout }}
  timeout: {{ . | quote }}
  {{- end }}
  {{- with $ga.auth }}
  auth:
    type: {{ .type | default "none" | quote }}
    {{- if .existingSecret }}
    token_file: {{ $tokenPath }}
    {{- end }}
  {{- end }}
  {{- with $ga.max_annotations_per_run }}
  max_annotations_per_run: {{ . }}
  {{- end }}
  {{- with $ga.tags }}
  tags:
{{ toYaml . | indent 4 }}
  {{- end }}
{{- end }}
{{- end -}}

{{/*
  Mount path for one sink's bearer-token Secret in the list (``sinks:``)
  form. The sink ``type`` is unique per config, so it makes a stable
  per-sink directory; ``_`` is not valid in a Kubernetes name so it is
  rewritten to ``-`` for the volume name (the mount path keeps it).
*/}}
{{- define "promanomaly.sinkAuthDir" -}}
{{- printf "/etc/promanomaly-sink-auth/%s" (. | replace "_" "-") -}}
{{- end -}}

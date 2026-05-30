{{/* Expand the name of the chart. */}}
{{- define "adapter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Create a default fully qualified app name. */}}
{{- define "adapter.fullname" -}}
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
{{- define "adapter.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Common labels applied to every resource. */}}
{{- define "adapter.labels" -}}
helm.sh/chart: {{ include "adapter.chart" . }}
{{ include "adapter.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels — must be stable across upgrades. */}}
{{- define "adapter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "adapter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* ServiceAccount name to use. */}}
{{- define "adapter.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "adapter.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* ConfigMap name: either the user-supplied existing CM or the one we render. */}}
{{- define "adapter.configMapName" -}}
{{- if .Values.existingConfigMap -}}
{{- .Values.existingConfigMap -}}
{{- else -}}
{{- printf "%s-config" (include "adapter.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* TLS secret name. */}}
{{- define "adapter.tlsSecretName" -}}
{{- if .Values.tls.existingSecret -}}
{{- .Values.tls.existingSecret -}}
{{- else -}}
{{- printf "%s-tls" (include "adapter.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Memoized self-signed serving certificate. Generated once on first call
and stashed on the shared root context so the Secret and both APIService
caBundles read the *same* pair within one `helm template` render (a fresh
genSignedCert per file would make the caBundles disagree with the served
cert). Access the result via `.adapterCerts.cert.Cert` /
`.adapterCerts.cert.Key` / `.adapterCerts.ca.Cert`.

This regenerates on every `helm upgrade`; for a long-lived rotation-free
cert use `tls.existingSecret` (e.g. cert-manager) — see the README.
*/}}
{{- define "adapter.certs" -}}
{{- if not .adapterCerts -}}
{{- $svc := include "adapter.fullname" . -}}
{{- $ns := .Release.Namespace -}}
{{- $cn := printf "%s.%s.svc" $svc $ns -}}
{{- $altNames := list $cn (printf "%s.%s.svc.cluster.local" $svc $ns) -}}
{{- $ca := genCA (printf "%s-ca" $svc) 3650 -}}
{{- $cert := genSignedCert $cn nil $altNames 3650 $ca -}}
{{- $_ := set . "adapterCerts" (dict "ca" $ca "cert" $cert) -}}
{{- end -}}
{{- end -}}

{{/*
The CA bundle (base64) the APIService should trust. Empty when
insecureSkipTLSVerify is set or when the operator supplied their own
secret without a caBundle value.
*/}}
{{- define "adapter.caBundle" -}}
{{- if .Values.tls.existingSecret -}}
{{- .Values.tls.caBundle -}}
{{- else -}}
{{- include "adapter.certs" . -}}
{{- b64enc .adapterCerts.ca.Cert -}}
{{- end -}}
{{- end -}}

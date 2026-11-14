{{/*
Chart name used for the app.kubernetes.io/name label. This is FIXED to
"animus-operator": the operator's own NetworkPolicy builder selects the
operator pod by `app.kubernetes.io/name: animus-operator` (see
crates/animus-operator/src/desired/mod.rs OPERATOR_APP_NAME), so the label
value must not follow nameOverride.
*/}}
{{- define "animus-operator.name" -}}
animus-operator
{{- end }}

{{/* Resource name prefix; nameOverride/fullnameOverride affect object names only. */}}
{{- define "animus-operator.fullname" -}}
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

{{- define "animus-operator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "animus-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "animus-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "animus-operator.labels" -}}
helm.sh/chart: {{ include "animus-operator.chart" . }}
{{ include "animus-operator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "animus-operator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "animus-operator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "animus-operator.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{- define "animus-operator.webhookServiceName" -}}
{{- printf "%s-webhook" (include "animus-operator.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "animus-operator.webhookTlsSecretName" -}}
{{- default (printf "%s-webhook-tls" (include "animus-operator.fullname" .) | trunc 63 | trimSuffix "-") .Values.webhook.tlsSecretName }}
{{- end }}

{{- define "animus-operator.webhookCertName" -}}
{{- printf "%s-webhook-cert" (include "animus-operator.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

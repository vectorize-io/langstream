{{/*
Expand the name of the chart.
*/}}
{{- define "sga.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "sga.fullname" -}}
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
{{- define "sga.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "sga.labels" -}}
helm.sh/chart: {{ include "sga.chart" . }}
{{ include "sga.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "sga.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sga.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "sga.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "sga.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role to use
*/}}
{{- define "sga.roleName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "sga.fullname" .) .Values.serviceAccount.role.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.role.name }}
{{- end }}
{{- end }}

{{/*
Create the name of the role binding to use
*/}}
{{- define "sga.roleBindingName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "sga.fullname" .) .Values.serviceAccount.roleBinding.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.roleBinding.name }}
{{- end }}
{{- end }}

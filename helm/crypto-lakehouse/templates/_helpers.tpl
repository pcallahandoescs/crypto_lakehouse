{{/*
Common labels applied to every object so `kubectl get all -l
app.kubernetes.io/part-of=crypto-lakehouse` and Helm's own tracking both work.
*/}}
{{- define "crypto-lakehouse.labels" -}}
app.kubernetes.io/part-of: crypto-lakehouse
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/*
Name of the Secret holding MinIO credentials — either a user-provided existing
Secret or the one this chart renders.
*/}}
{{- define "crypto-lakehouse.secretName" -}}
{{- if .Values.credentials.existingSecret -}}
{{- .Values.credentials.existingSecret -}}
{{- else -}}
minio-credentials
{{- end -}}
{{- end -}}

{{/*
The MinIO access/secret-key env pair the Spark jobs and serving read, sourced
from the credentials Secret. Include with `nindent`.
*/}}
{{- define "crypto-lakehouse.minioEnv" -}}
- name: MINIO_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "crypto-lakehouse.secretName" . }}
      key: MINIO_ROOT_USER
- name: MINIO_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "crypto-lakehouse.secretName" . }}
      key: MINIO_ROOT_PASSWORD
{{- end -}}

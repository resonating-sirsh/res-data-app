{{- define "permissiveMerge" -}}
  {{- $rv := dict -}}
  {{- if not (empty .) -}}
    {{- range $arg := . -}}
      {{- if not (empty $arg) -}}
        {{- $rv := merge $rv $arg -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- $rv -}}
{{- end -}}
{{- define "permissiveMergeOverwrite" -}}
  {{- $rv := dict -}}
  {{- if not (empty .) -}}
    {{- range $arg := . -}}
      {{- if not (empty $arg) -}}
        {{- $rv := mergeOverwrite $rv $arg -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
  {{- $rv -}}
{{- end -}}

{{- define "copyContents" -}}
  {{- if kindIs "slice" . -}}
  {{- range . }}
  - {{- include "copyContents" . | indent 2 }}
  {{- end -}}
  {{- else if kindIs "map" . -}}
  {{- range $key, $value := . }}
  {{ $key }}: {{- include "copyContents" $value | indent 2 -}}
  {{- end -}}
  {{- else if not (empty .) -}}
  {{ . }}
  {{- end -}}
{{- end -}}

{{- define "defaultImageUri" -}}
{{ .Values.global.ecrRepo }}/res-data_{{ .Values.global.environment }}_{{ .Values.deployment.name }}_{{ .Values.deployment.folder }}:{{default "latest" .Values.commitHash  }}
{{- end -}}

{{- define "containers" -}}
  {{- range (default (list (dict "name" .Values.deployment.name)) .Values.deployment.containers) }}
- name: {{ .name }}
  {{- if (get . "image") }}
  image: {{ .image }}
  {{- else }}
  image: {{ include "defaultImageUri" $ | quote }}
  {{- end }}
  imagePullPolicy: Always
  env:
    {{- 
      range $key, $value :=
        (mergeOverwrite (dict) 
          (coalesce $.Values.baseEnvs (dict))
          (coalesce $.Values.deployment.env (dict)) 
          (coalesce (get . "env") (dict))
          (dict
            "RES_ENV" $.Values.global.environment 
            "RES_APP_NAME" $.Values.deployment.name
            "RES_NAMESPACE" $.Values.deployment.folder)
          (dict))
    }}
    - name: {{ $key }}
      value: {{ $value | quote }}
    {{- end }}
  {{- include "copyContents" (mergeOverwrite (dict) (coalesce $.Values.deployment.containerDefaults (dict)) (omit . "name" "image" "imagePullPolicy" "env")) }}
  {{- end }}
{{- end -}}

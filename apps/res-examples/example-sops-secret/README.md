# SOPS Secrets Example

Example of encrypted secrets using [`SOPS`](https://github.com/mozilla/sops).

## Install SOPS

To use this you need to install sops:
```sh
brew update && brew install sops
```

## Create a secret

**SUPER SUPER SUPER IMPORTANT!!!**

DO NOT COMMIT ANY SECRET FILES!! TO AVOID DOING SO, DO NOT CREATE ANY SECRET
FILES INSIDE ANY GIT REPOSITORY!!

1. Outside of the repo create a yaml file with the contents of the secret you
   want to encrypt. NB: only the contents of `data` or `stringData` will be
   encrypted by default, so make sure any sensitive information is contained in
   those parts of the secret.

```yaml
# ~/secret-files-do-not-commit/my-secret.yaml

apiVersion: v1
data:
  FOO: J3NlY3JldCc=
  secret-file.txt: c2VjcmV0Cg==
kind: Secret
type: Opaque
```

2. `cd` into the `res-data-platform` repo and invoke SOPS. This uses the profile
   stored in the `.sops.yaml` file. First confirm from the output that the
   sensitive information in your secret has been correctly encrypted.

```sh
~ $ cd ~/path/to/res-data-platform

res-data-platform $ sops -e ~/secret-files-do-not-commit/my-secret.yaml

apiVersion: v1
data:
    FOO: ENC[AES256_GCM,data:H5wWOyIFimTou6lJ,iv:AVTwS++hK3bsG+URhnG8mbiSsh3lClQS/LhFWUTU/9A=,tag:ve9jwlyqyAHbG44ImNKT8A==,type:str]
    secret-file.txt: ENC[AES256_GCM,data:pLdPug5TWuHjNVTX,iv:5iIRKhkOuLzvSdOMG4vhCBprhirbONu6R2s9i+Xhr2w=,tag:3Zt/Ga8iAtldkSnVD+m1Kw==,type:str]
kind: Secret
type: Opaque
sops:
    kms:
        - arn: arn:aws:kms:us-east-1:286292902993:key/8dc23836-ab89-4581-a981-14c3c646c1ba
          created_at: "2022-07-05T19:11:01Z"
          enc: AQICAHgPdAvulU5/aXIXPDm3XYIFQRuoxDaKt3ktMkWdqy/j5wGyzF59tjmZ1APFs9g+Oy3iAAAAfjB8BgkqhkiG9w0BBwagbzBtAgEAMGgGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMWWgyP6KfgQi31RpKAgEQgDv8z5Ol90xB3qgbFsi3NvIShwdvkqsXsL2b+3VX+dNW17ADoluGSUsjhye8PI/32BLRJpChh8L5oyaVTg==
          aws_profile: ""
    gcp_kms: []
    azure_kv: []
    hc_vault: []
    age: []
    lastmodified: "2022-07-05T19:11:01Z"
    mac: ENC[AES256_GCM,data:L0MVSNf2UcqMhXTMeorB+C5Dv1OzAef/5ANuYFH2OYYRIH1e9VU0u8X4S4SnMXPZZQ7uLcQENJWQE3oOxLyvZLubGP9+ossrI7WOh/oIkgyf77poPkhAsc3LekLWUWuENjKVvaJmO9aYS5kYT99NNMlIbb0Hi7rCM0Agdsm5kDM=,iv:nXL4cU6J3xETytRgOxZ5fcPmKxjLE7Y80ramtdy+VSM=,tag:DOB8F7WdPMw7nQzIGluJpA==,type:str]
    pgp: []
    encrypted_regex: ^(data|stringData)$
    version: 3.7.3
```

3. This looks good, all of the sensitive stuff in `data` was encrypted. Now we
   can do the same thing but write it to a file using a shell redirect. We add a
   `.enc` to the extension to let people know that this yaml file is encrypted.


```sh
res-data-platform $ sops -e ~/secret-files-do-not-commit/my-secret.yaml \
    > ./apps/res-examples/example-sops-secret/base/secret.enc.yaml
```

4. This file can be added in git and committed. Make sure you don't commit the
   original, unencrypted secret!!


## Use the secret in a Kustomize app via [`SopsSecretGenerator`](https://github.com/goabout/kustomize-sopssecretgenerator)

If you want to test this locally in kustomize, make sure you have kustomize >
4.5.4 installed. Then follow the [installation
instructions](https://github.com/goabout/kustomize-sopssecretgenerator#installation).

To make kustomize in argocd correctly decrypt secrets, simply point it at the
SopsSecretGenerator resource:

```yaml
apiVersion: goabout.com/v1beta1
kind: SopsSecretGenerator
metadata:
  name: my-secret
files:
  - secret.enc.yaml
type: Opaque
```

```yaml

```
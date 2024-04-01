# res-data-platform

## Todo

- check for changed files before the black etc
- run tests on the docker image in low code
- find a way to do DRY on the existing yaml to make it simpler
- kick off the infra repo PR and close it when our PR closes. That will be a simple ephemeral app
- scale down all the dev things to 0 for now - these can become staging apps
- confirm all the tags
- have the infra app deploy to master for some test apps

```
docker buildx build -t 286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:latest \
--cache-to mode=max,image-manifest=true,oci-mediatypes=true,type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache \
--cache-from type=registry,ref=286292902993.dkr.ecr.us-east-1.amazonaws.com/res-data-app:cache ../../../

docker push <account-id>.dkr.ecr.<my-region>.amazonaws.com/buildkit-test:image
```

## Changes

We change how the base image is used in Apps so this is required for migration. Use the image:{sha} only from the build

<https://www.jmsbrdy.com/blog/previews-with-argo>

# build the main docker image with caching after running tests

# if apps have changed, also build the docker under their folder

# then call the infra repo to update its image tag and argo will do the rest since we will one time setup the applications

# later we can add DB migrations and but we probably will discontinue them and switch to postgres migration and schema from pydantic types

# app_list=$(git diff --name-only origin/main  | grep '^apps/' | awk -F/ '{print $1 "/" $2 "/" $3}' | sort -u | jq -R -s 'split("\n") | map(select(. != ""))' )

# a=$($app_list | jq -r 'join(",")')

# git diff --name-only origin/main  | grep '^apps/' | awk -F/ '{print $1 "/" $2 "/" $3}' | sort -u | jq -R -s 'split("\n") | map(select(. != ""))' | jq -cR '.' | sed 's/"/\\"/g'

# <https://stackoverflow.com/questions/59977364/github-actions-how-use-strategy-matrix-with-script>

# <https://hub.github.com/hub-pull-request.1.html>

# <https://github.com/marketplace/actions/setup-hub>

# <https://hub.github.com/>

# # sudo apt install hub

# hub pull-request --base main --message "merging infra for app changes ${GITHUB_SHA:0:7}"

      - uses: actions/github-script@v7
        id: get_pr_data
        #when we are merging we can go back and get the pull request data
        #if: github.event_name != 'pull_request'
        with:
          script: |
            return (
              await github.rest.repos.listPullRequestsAssociatedWithCommit({
                commit_sha: context.sha,
                owner: context.repo.owner,
                repo: context.repo.repo,
              })
            ).data[0];

      # - name: Pull Request data
      #   run: |
      #     echo '${{ fromJson(steps.get_pr_data.outputs.result).number }}'
      #     echo '${{ fromJson(steps.get_pr_data.outputs.result).title }}'

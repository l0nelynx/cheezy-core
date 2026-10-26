# Upstream Alpha sync

## Authentication

Create a fine-grained personal access token with resource owner `l0nelynx`,
repository access **Only select repositories → cheezy-core**, and these
repository permissions:

| Permission | Access | Used for |
| --- | --- | --- |
| Contents | Read and write | Fetch and push mirror and PR branches |
| Workflows | Read and write | Mirror upstream changes to `.github/workflows` |
| Pull requests | Read and write | Find existing PRs and create sync PRs |
| Issues | Read and write | Notify when vernesong is stale and MetaCubeX is ahead |
| Metadata | Read-only (automatic) | Repository metadata |

Save the token in **Settings → Secrets and variables → Actions → New repository
secret** as `UPSTREAM_SYNC_TOKEN`. Use a secret, not an Actions variable.
Renew the secret before the token expires.

The workflow passes this secret to `actions/checkout` as `with.token` for Git
authentication and to the sync script as `GH_TOKEN` for GitHub CLI operations.
The YAML `permissions` block controls only the built-in `GITHUB_TOKEN`; it does
not grant or restrict the PAT's permissions. Actions write permission is not
required. PAT-authenticated pushes and PRs can trigger other workflows.

After these changes reach `cheezy-wap`, run **Sync upstream Alpha branches**
manually with `dry_run=false` to push branches and create a PR. The default
manual dry run only validates upstream and does not exercise write permissions.

## Conflicts

A clean merge produces the normal sync PR. A merge conflict produces a draft
PR from an upstream-based branch into `cheezy-wap`, with a list of conflicting
files and local resolution commands. No conflict markers are committed.
The branch includes a proposed lock-file update; integration is complete only
after resolution and merging into `cheezy-wap`.

Resolve in GitHub when its editor supports the conflict, or follow the PR's
local commands. When merging `origin/cheezy-wap` into the conflict PR branch,
**ours** is upstream and **theirs** is Cheezy Core. Preserve the project changes
and reconcile the lock file. Mark the draft ready after resolving conflicts.

Use **Create a merge commit** for the final PR merge to preserve upstream
ancestry. Squash/rebase merging can cause future syncs to propose already
integrated upstream commits again. An existing open PR for the same upstream
SHA is left intact on subsequent runs, preserving manual resolutions.

The separate stale-upstream issue remains enabled. Conflict issues are no
longer created; existing issues are not automatically closed.

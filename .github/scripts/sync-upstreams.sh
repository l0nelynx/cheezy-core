#!/usr/bin/env bash
set -euo pipefail

git fetch --no-tags https://github.com/vernesong/mihomo.git \
  refs/heads/Alpha:refs/remotes/upstream/vernesong-alpha
git fetch --no-tags https://github.com/MetaCubeX/mihomo.git \
  refs/heads/Alpha:refs/remotes/upstream/metacubex-alpha

verify_ref() {
  local ref="$1" trusted="$2"
  test "$(git show "$ref:go.mod" | sed -n '1p')" = "module github.com/metacubex/mihomo"
  for path in adapter config constant transport tunnel; do
    git cat-file -e "$ref:$path"
  done
  git cat-file -e "$trusted^{commit}"
  test -n "$(git merge-base "$ref" "$trusted")"
}

verify_ref refs/remotes/upstream/vernesong-alpha \
  "$(jq -r '.primary.trusted_root' upstream-lock.json)"
verify_ref refs/remotes/upstream/metacubex-alpha \
  "$(jq -r '.fallback.trusted_root' upstream-lock.json)"

original="$(git rev-parse HEAD)"
for ref in upstream/vernesong-alpha upstream/metacubex-alpha; do
  git checkout --detach "$ref"
  go test ./adapter/outbound ./component/dialer ./config
done
git checkout --detach "$original"

dry_run="${INPUT_DRY_RUN:-false}"
if [[ "${EVENT_NAME:-schedule}" == "workflow_dispatch" && "$dry_run" == "true" ]]; then
  echo "Dry run complete; no refs were pushed."
  exit 0
fi

git push origin +refs/remotes/upstream/vernesong-alpha:refs/heads/source/vernesong-alpha
git push origin +refs/remotes/upstream/metacubex-alpha:refs/heads/source/metacubex-alpha

source="${INPUT_SOURCE:-vernesong}"
vernesong_date="$(git show -s --format=%ct upstream/vernesong-alpha)"
age_days="$(( ($(date +%s) - vernesong_date) / 86400 ))"
metacubex_ahead=false
if ! git merge-base --is-ancestor upstream/metacubex-alpha upstream/vernesong-alpha; then
  metacubex_ahead=true
fi

if [[ "${EVENT_NAME:-schedule}" == "schedule" ]]; then
  source=vernesong
  if [[ "$metacubex_ahead" == true && "$age_days" -ge 14 ]]; then
    title="MetaCubeX Alpha is ahead of vernesong Alpha"
    if ! gh issue list --state open --search "$title in:title" --json title --jq 'length > 0' | grep -q true; then
      gh issue create --title "$title" \
        --body "MetaCubeX Alpha contains commits not present in vernesong Alpha. Vernesong has been idle for ${age_days} days."
    fi
  fi
  if [[ "$metacubex_ahead" == true && "$age_days" -ge 30 ]]; then
    source=metacubex
  fi
fi

if [[ "$source" == "metacubex" ]]; then
  source_ref=upstream/metacubex-alpha
else
  source=vernesong
  source_ref=upstream/vernesong-alpha
fi
source_sha="$(git rev-parse "$source_ref")"
short_sha="${source_sha:0:12}"

if git merge-base --is-ancestor "$source_ref" HEAD; then
  echo "cheezy-wap already contains $source_sha"
  exit 0
fi

branch="sync/${source}-${short_sha}"
if gh pr list --state open --head "$branch" --json number --jq 'length > 0' | grep -q true; then
  exit 0
fi

git config user.name github-actions[bot]
git config user.email 41898282+github-actions[bot]@users.noreply.github.com
git checkout -b "$branch"
conflicted=false
conflict_files=""
if ! git merge --no-ff --no-edit "$source_ref"; then
  conflict_files="$(git diff --name-only --diff-filter=U)"
  if [[ -z "$conflict_files" ]]; then
    echo "Merge failed without conflicted files; refusing to create a conflict PR." >&2
    exit 1
  fi
  git merge --abort
  conflicted=true
  # Expose actual conflicts with the PR base without committing conflict markers.
  git checkout -B "$branch" "$source_ref"
  git show "$original:upstream-lock.json" > upstream-lock.json
fi

tmp="$(mktemp)"
if [[ "$source" == "metacubex" ]]; then
  jq --arg source "$source" --arg sha "$source_sha" \
    '.active_source = $source | .fallback.integrated_sha = $sha' upstream-lock.json > "$tmp"
else
  jq --arg source "$source" --arg sha "$source_sha" \
    '.active_source = $source | .primary.integrated_sha = $sha' upstream-lock.json > "$tmp"
fi
mv "$tmp" upstream-lock.json
git add upstream-lock.json
if [[ "$conflicted" == true ]]; then
  git commit -m "Record proposed ${source} Alpha sync ${short_sha}"
else
  git commit --amend --no-edit
fi
git push origin "$branch"

draft_args=()
if [[ "$source" == "metacubex" || "$conflicted" == true ]]; then
  draft_args+=(--draft)
fi
body="$(mktemp)"
trap 'rm -f "$body"' EXIT
{
  printf 'Validated Alpha-only upstream sync.\n\nSource: `%s`\nSource SHA: `%s`\nTarget SHA at validation: `%s`\n' "$source" "$source_sha" "$original"
  printf '\nManual review and all Cheezy Core checks are required. Merge this PR with a merge commit to preserve upstream ancestry.\n'
  if [[ "$conflicted" == true ]]; then
    printf '\n## Conflicts require resolution\n\nThe automatic merge into `cheezy-wap` conflicted. This draft contains the upstream commits and a proposed lock-file update; no conflict markers were committed.\n\nConflicting files at validation:\n\n'
    while IFS= read -r path; do
      printf -- '- `%s`\n' "$path"
    done <<< "$conflict_files"
    printf '\nResolve using GitHub if its conflict editor is available, or locally:\n\n```bash\ngit fetch origin\ngit switch --track origin/%s\ngit merge origin/cheezy-wap\n# Resolve conflicts, then stage each resolved file.\ngit commit\ngit push origin HEAD\n```\n' "$branch"
    printf '\nWhen merging the base into this branch, ours is upstream and theirs is Cheezy Core. Preserve Cheezy-specific changes and reconcile `upstream-lock.json` with the source SHA above. Then mark this PR ready for review.\n'
  fi
} > "$body"

gh pr create "${draft_args[@]}" --base cheezy-wap --head "$branch" \
  --title "Sync ${source} Alpha ${short_sha}" \
  --body-file "$body"

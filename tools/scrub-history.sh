#!/usr/bin/env bash
# Rewrite history to remove the absolute local paths that bench/results/environment.json carried
# between b5d4a13 and f4ebdcf. Works on a fresh mirror clone, never on the repository you are in.
#
#   tools/scrub-history.sh <source-url-or-path> <work-dir>
#
# The work dir receives a mirror clone named "mirror" with the rewritten refs, a commit map
# (old hash, new hash) and a verification report. Nothing is pushed; see the runbook in the
# pull request that added this script. Requires git >= 2.36 and git-filter-repo.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 <source-url-or-path> <work-dir>" >&2
  exit 2
fi
SOURCE=$1
WORK=$(mkdir -p "$2" && cd "$2" && pwd)
MIRROR=$WORK/mirror
REPORT=$WORK/report.txt
LAST_CLEAN=4c9191ae905430266f63efcee65c591bc3f0e95f

git filter-repo --version >/dev/null 2>&1 || { echo "git-filter-repo is not installed (pip install git-filter-repo)" >&2; exit 1; }
[ -e "$MIRROR" ] && { echo "$MIRROR already exists; use an empty work dir" >&2; exit 1; }

# The one blob that carried the paths. The leaked strings themselves are never written into this
# file, so the scan below cannot match the script's own source once it is part of the history.
LEAKED_BLOB=d1a7b64e8201382dd2c1be9ffa28c7187bb6e5c8

# A checkout path under any user's home, a session-scoped temporary directory, a Windows profile
# path, or a UUID. Documentation examples such as a generic /home/{user}/ do not match.
LEAK_PATTERN='/(home|Users)/[A-Za-z0-9_.-]+/floodgate/|/tmp/claude-[0-9]+|/mnt/[a-z]/Users/|[Cc]:\\\\Users|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
scan() {
  git -C "$1" rev-list --all --objects | awk '{print $1}' \
    | git -C "$1" cat-file --batch-check='%(objecttype) %(objectname)' | awk '$1=="blob"{print $2}' | sort -u \
    | while read -r blob; do
        git -C "$1" cat-file -p "$blob" | grep -nE "$LEAK_PATTERN" | sed "s/^/$blob:/" || true
      done
}

# Build the replacement map from the leaked blob itself: every absolute path value becomes the
# path relative to the checkout when it points inside it, and the basename otherwise. Those are
# the forms the scrubbed file already uses, so the rewrite reproduces it exactly.
build_replacements() {
  git -C "$MIRROR" cat-file -p "$LEAKED_BLOB" \
    | grep -oE '"/[^"]+"' | tr -d '"' | sort -u \
    | while read -r path; do
        case "$path" in
          /home/*/floodgate/*) echo "$path==>${path#/home/*/floodgate/}" ;;
          *) echo "$path==>$(basename "$path")" ;;
        esac
      done > "$WORK/replacements.txt"
  [ -s "$WORK/replacements.txt" ] || { echo "blob $LEAKED_BLOB holds no absolute path; nothing to rewrite" >&2; exit 1; }
  echo "replacements (old==>new), derived from blob $LEAKED_BLOB:"
  sed 's/^/  /' "$WORK/replacements.txt"
}

git clone --quiet --mirror "$SOURCE" "$MIRROR"
# Pull request refs cannot be pushed back, and scrubbed/* branches are the product of an earlier
# run of this script; both would only confuse the counts below, so drop them locally.
git -C "$MIRROR" for-each-ref --format='%(refname)' 'refs/pull/' 'refs/heads/scrubbed/' \
  | xargs -r -n1 git -C "$MIRROR" update-ref -d

# Snapshot of the history before the rewrite, for the verification below.
git -C "$MIRROR" for-each-ref --format='%(refname) %(objectname)' refs/heads refs/tags > "$WORK/refs-before.txt"
git -C "$MIRROR" log --all --format='%an|%ae|%ad|%cn|%ce|%cd|%B%x00' --date=raw > "$WORK/log-before.txt"
git -C "$MIRROR" log --all --format='%H %T' > "$WORK/trees-before.txt"
git -C "$MIRROR" cat-file -e "$LEAKED_BLOB" || { echo "blob $LEAKED_BLOB is not in $SOURCE; nothing to scrub" >&2; exit 1; }
build_replacements | tee "$REPORT"
echo "scan before rewrite:" | tee -a "$REPORT"
scan "$MIRROR" | tee "$WORK/scan-before.txt" | tee -a "$REPORT" | sed 's/^/  before: /'
cut -d: -f1 "$WORK/scan-before.txt" | sort -u > "$WORK/leaked-blobs.txt"

# Every commit is SSH-signed and fast-export drops signatures, so only the commits after the
# last clean one are fed to filter-repo; everything up to $LAST_CLEAN keeps its hash and signature.
mapfile -t BRANCHES < <(git -C "$MIRROR" for-each-ref --format='%(refname)' refs/heads refs/tags)
git -C "$MIRROR" filter-repo --force --replace-text "$WORK/replacements.txt" \
  --preserve-commit-hashes --replace-refs delete-no-add --quiet \
  --refs "^$LAST_CLEAN" "${BRANCHES[@]}"
cp "$MIRROR/filter-repo/commit-map" "$WORK/commit-map.txt"

git -C "$MIRROR" for-each-ref --format='%(refname) %(objectname)' refs/heads refs/tags > "$WORK/refs-after.txt"
git -C "$MIRROR" log --all --format='%an|%ae|%ad|%cn|%ce|%cd|%B%x00' --date=raw > "$WORK/log-after.txt"
git -C "$MIRROR" log --all --format='%H %T' > "$WORK/trees-after.txt"

FAILURES=0
fail() {
  echo "  FAIL: $1"
  FAILURES=$((FAILURES + 1))
}
verify() {
  echo
  echo "refs before -> after:"
  join <(sort "$WORK/refs-before.txt") <(sort "$WORK/refs-after.txt") | awk '{printf "  %s %s -> %s%s\n", $1, $2, $3, ($2==$3 ? " (unchanged)" : "")}'
  echo
  echo "changed commits (old -> new):"
  awk 'NR>1 && $1!=$2 {printf "  %s -> %s\n", $1, $2}' "$WORK/commit-map.txt"
  echo "  changed: $(awk 'NR>1 && $1!=$2' "$WORK/commit-map.txt" | wc -l), unchanged: $(awk 'NR>1 && $1==$2' "$WORK/commit-map.txt" | wc -l)"
  echo
  echo "commits up to $LAST_CLEAN keep their hashes:"
  if git -C "$MIRROR" rev-parse --verify --quiet "$LAST_CLEAN^{commit}" >/dev/null; then
    echo "  $LAST_CLEAN present, $(git -C "$MIRROR" rev-list --count "$LAST_CLEAN") commits reachable from it, byte-identical by hash, signatures kept"
  else
    fail "$LAST_CLEAN is missing from the rewritten history"
  fi
  echo
  echo "commit count: before $(wc -l < "$WORK/trees-before.txt"), after $(wc -l < "$WORK/trees-after.txt")"
  [ "$(wc -l < "$WORK/trees-before.txt")" = "$(wc -l < "$WORK/trees-after.txt")" ] || fail "commit count changed"
  if cmp -s "$WORK/log-before.txt" "$WORK/log-after.txt"; then
    echo "authors, committers, both dates and messages: identical for every commit"
  else
    fail "author, committer, date or message differs"
  fi
  echo
  echo "tree of each ref tip, before vs after (a tip whose tree holds no leaked blob must keep its tree):"
  while read -r ref old; do
    new=$(awk -v r="$ref" '$1==r {print $2}' "$WORK/refs-after.txt")
    told=$(awk -v h="$old" '$1==h {print $2}' "$WORK/trees-before.txt")
    tnew=$(awk -v h="$new" '$1==h {print $2}' "$WORK/trees-after.txt")
    if git -C "$MIRROR" ls-tree -r "$old" | awk '{print $3}' | grep -qxFf "$WORK/leaked-blobs.txt"; then
      printf "  %s tree %s -> %s (tip held a leaked blob, so it changes)\n" "$ref" "$told" "$tnew"
      [ "$told" != "$tnew" ] || fail "tree of $ref still holds the leaked blob"
    else
      printf "  %s tree %s -> %s %s\n" "$ref" "$told" "$tnew" "$([ "$told" = "$tnew" ] && echo same || echo DIFFERENT)"
      [ "$told" = "$tnew" ] || fail "tree of $ref changed although it held no leaked blob"
    fi
  done < "$WORK/refs-before.txt"
  echo
  echo "trees that changed (one per rewritten commit whose tree held the old blob):"
  paste -d' ' <(awk '{print $2}' "$WORK/trees-before.txt") <(awk '{print $2}' "$WORK/trees-after.txt") | awk '$1!=$2' | wc -l | sed 's/^/  /'
  echo
  echo "scan after rewrite (must be empty):"
  local leftover
  leftover=$(scan "$MIRROR")
  if [ -n "$leftover" ]; then
    echo "$leftover" | sed 's/^/  /'
    fail "leaked strings remain after the rewrite"
  fi
  echo "  (end of scan)"
  echo
  if [ "$FAILURES" -eq 0 ]; then
    echo "verification passed"
  else
    echo "verification FAILED ($FAILURES checks); do not push this rewrite"
  fi
}
verify | tee -a "$REPORT"
echo
echo "rewritten mirror: $MIRROR"
echo "report: $REPORT"
grep -q '^verification passed$' "$REPORT"

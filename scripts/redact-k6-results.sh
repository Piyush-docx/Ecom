#!/usr/bin/env bash
# Redact JWTs from k6 raw output.
#
# k6's raw dumps embed whole request bodies, so a load run leaves live tokens
# for the synthetic test users in deploy/k6/results/*.json. They are signed with
# whatever JWT_SECRET the run used and are worthless to an attacker, but
# committing credentials of any kind is a habit worth not having -- and the
# files are rewritten by every run, so this needs doing every time.
#
# Run manually, or install as a pre-commit hook:
#   ln -s ../../scripts/redact-k6-results.sh .git/hooks/pre-commit
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
results="$root/deploy/k6/results"
[ -d "$results" ] || exit 0

changed=0
for f in "$results"/*.json "$results"/*.log; do
  [ -e "$f" ] || continue
  if grep -qE 'eyJhbGciOi[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+' "$f" 2>/dev/null; then
    perl -pi -e 's/eyJhbGciOi[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/<JWT-REDACTED>/g' "$f"
    echo "redacted JWT(s) in ${f#$root/}"
    git -C "$root" add "$f" 2>/dev/null || true
    changed=1
  fi
done

[ "$changed" = "1" ] && echo "k6 results were redacted and re-staged."
exit 0

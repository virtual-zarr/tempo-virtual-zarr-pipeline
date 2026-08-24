#!/usr/bin/env bash
# Fail if a tracked env file assigns a value to a key that belongs in the
# gitignored .env.local (see .env.local.sample). Invoked by pre-commit with
# the staged .env* filenames as arguments.
set -euo pipefail
DENY='ACCOUNT_ID|AWS_PROFILE|OWNER|CLIENT|ALARM_EMAIL|EARTHDATA_SECRET_ARN|VPC_ID'
status=0
for f in "$@"; do
  if grep -nE "^($DENY)=." "$f"; then
    echo "$f: semi-sensitive key has a value; move it to .env.local (gitignored)." >&2
    status=1
  fi
done
exit $status

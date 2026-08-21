#!/usr/bin/env bash
#
# Build a backfill inventory in-region via the stack's CodeBuild project.
#
# The DAAC's temporary S3 credentials only work from us-west-2, so running
# build_backfill_inventory.py locally with --access direct fails. This ships
# the committed repo (git archive HEAD) as the project's source zip, starts a
# build, and waits for it — every run is pinned by a commit plus the in-repo
# buildspec (scripts/inventory_buildspec.yml).
#
# Usage:
#   scripts/build_inventory_remote.sh -e ENV_FILE [-m MAX_COUNT] [-u S3_URI]
#
# Examples:
#   # 50-granule trial inventory for the hcho stack:
#   scripts/build_inventory_remote.sh -e .env_hcho -m 50
#   # Full inventory to an explicit key:
#   scripts/build_inventory_remote.sh -e .env_hcho \
#     -u s3://my-bucket/tempo/hcho/inventory/hcho.json
#
# COLLECTION and the default S3_URI are baked into the project by the CDK
# stack, so only the env file is required.

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: build_inventory_remote.sh -e ENV_FILE [-m MAX_COUNT] [-u S3_URI]

  -e ENV_FILE   per-collection env file (e.g. .env_hcho); supplies the stack
                name, region, bucket, prefixes, and AWS profile
  -m MAX_COUNT  keep only the N most recent granules (trial runs)
  -u S3_URI     override the inventory destination baked into the project
EOF
  exit 2
}

ENV_FILE=""
MAX_COUNT=""
S3_URI=""
while getopts ":e:m:u:h" opt; do
  case "$opt" in
    e) ENV_FILE="$OPTARG" ;;
    m) MAX_COUNT="$OPTARG" ;;
    u) S3_URI="$OPTARG" ;;
    h) usage ;;
    :) echo "Error: -$OPTARG requires a value." >&2; usage ;;
    \?) echo "Error: unknown option -$OPTARG." >&2; usage ;;
  esac
done

[ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ] || { echo "Error: pass -e ENV_FILE." >&2; usage; }

env_get() {
  grep -E "^$1=" "$ENV_FILE" | tail -n1 | cut -d= -f2- | sed 's/^["'\'']//;s/["'\'']$//'
}

STACK="$(env_get STACK_NAME)"
REGION="$(env_get ACCOUNT_REGION)"
BUCKET="$(env_get ICECHUNK_BUCKET)"
PREFIX="$(env_get INVENTORY_PREFIX)"
[ -n "$PREFIX" ] || PREFIX="$(env_get S3_PREFIX)/inventory"
[ -n "$STACK" ] && [ -n "$BUCKET" ] || {
  echo "Error: STACK_NAME and ICECHUNK_BUCKET must be set in $ENV_FILE." >&2
  exit 2
}
if [ -z "${AWS_PROFILE:-}" ] && [ -n "$(env_get AWS_PROFILE)" ]; then
  export AWS_PROFILE="$(env_get AWS_PROFILE)"
fi
REGION_ARGS=()
[ -n "$REGION" ] && REGION_ARGS=(--region "$REGION")

PROJECT="$(aws cloudformation describe-stacks "${REGION_ARGS[@]}" \
  --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='InventoryBuildProject'].OutputValue" \
  --output text)"
if [ -z "$PROJECT" ] || [ "$PROJECT" = "None" ]; then
  echo "Error: InventoryBuildProject output not found on stack '$STACK'." >&2
  exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
  echo "Warning: uncommitted changes are NOT included (the build runs git HEAD)." >&2
fi
ZIP="$(mktemp -t inventory-source-XXXXXX.zip)"
trap 'rm -f "$ZIP"' EXIT
git archive --format=zip -o "$ZIP" HEAD
aws s3 cp "$ZIP" "s3://$BUCKET/$PREFIX/source.zip" "${REGION_ARGS[@]}" >&2

OVERRIDES=(name=MAX_COUNT,value="$MAX_COUNT",type=PLAINTEXT)
[ -n "$S3_URI" ] && OVERRIDES+=(name=S3_URI,value="$S3_URI",type=PLAINTEXT)
BUILD_ID="$(aws codebuild start-build "${REGION_ARGS[@]}" \
  --project-name "$PROJECT" \
  --environment-variables-override "${OVERRIDES[@]}" \
  --query 'build.id' --output text)"
echo "Started build $BUILD_ID (commit $(git rev-parse --short HEAD))" >&2

while :; do
  STATUS="$(aws codebuild batch-get-builds "${REGION_ARGS[@]}" \
    --ids "$BUILD_ID" --query 'builds[0].buildStatus' --output text)"
  [ "$STATUS" = "IN_PROGRESS" ] || break
  sleep 30
done
echo "Build finished: $STATUS" >&2
if [ "$STATUS" != "SUCCEEDED" ]; then
  echo "Logs: aws logs tail /aws/codebuild/$PROJECT ${REGION_ARGS[*]}" >&2
  exit 1
fi

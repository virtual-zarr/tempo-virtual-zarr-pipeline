#!/usr/bin/env bash
#
# Start a backfill Step Functions execution.
#
# Resolves the state machine ARN from the stack's `BackfillStateMachineArn`
# CloudFormation output, then starts an execution with the given name and
# inventory URI.
#
# Usage:
#   scripts/start_backfill.sh [-s STACK] [-r REGION] [-e ENV_FILE] \
#       <execution-name> <inventory-uri>
#
# Examples:
#   scripts/start_backfill.sh -s tempo-hcho -r us-west-2 \
#     hcho-backfill-20260820 s3://my-bucket/tempo/hcho/inventory/hcho.json
#
#   # Take the stack and region from one of the per-collection env files:
#   scripts/start_backfill.sh -e .env_hcho \
#     hcho-backfill-20260820 s3://my-bucket/tempo/hcho/inventory/hcho.json
#
# Stack and region are resolved in this order, first hit wins:
#   1. -s / -r flags
#   2. STACK / REGION environment variables
#   3. STACK_NAME / ACCOUNT_REGION in the -e env file
# The stack must resolve somehow; the region may fall through to the AWS CLI's
# configured default.

set -euo pipefail

usage() {
  cat >&2 <<'EOF'
Usage: start_backfill.sh [-s STACK] [-r REGION] [-e ENV_FILE] [execution-name] <inventory-uri>

  -s STACK     CloudFormation stack name to read BackfillStateMachineArn from
  -r REGION    AWS region the stack is deployed in
  -e ENV_FILE  read STACK_NAME / ACCOUNT_REGION from this file (e.g. .env_hcho)

  With no execution-name, one is generated as <stack>-backfill-<UTC timestamp>,
  which is always unique — Step Functions rejects a reused name for 90 days.

  e.g. start_backfill.sh -e .env_hcho s3://my-bucket/tempo/hcho/inventory/hcho.json
EOF
  exit 2
}

STACK_ARG=""
REGION_ARG=""
ENV_FILE=""
while getopts ":s:r:e:h" opt; do
  case "$opt" in
    s) STACK_ARG="$OPTARG" ;;
    r) REGION_ARG="$OPTARG" ;;
    e) ENV_FILE="$OPTARG" ;;
    h) usage ;;
    :) echo "Error: -$OPTARG requires a value." >&2; usage ;;
    \?) echo "Error: unknown option -$OPTARG." >&2; usage ;;
  esac
done
shift $((OPTIND - 1))

case $# in
  1) EXECUTION_NAME=""; INVENTORY_URI="$1" ;;  # name generated after STACK resolves
  2) EXECUTION_NAME="$1"; INVENTORY_URI="$2" ;;
  *) usage ;;
esac

case "$INVENTORY_URI" in
  s3://*) ;;
  *) echo "Error: inventory must be an s3:// URI, got '$INVENTORY_URI'." >&2; exit 2 ;;
esac

# Step Functions rejects these after the describe-stacks round trip; catching it
# here keeps the failure adjacent to the typo.
if [ -n "$EXECUTION_NAME" ] && { [ ${#EXECUTION_NAME} -gt 80 ] || [[ "$EXECUTION_NAME" =~ [[:space:]] ]]; }; then
  echo "Error: execution name must be <=80 chars with no whitespace." >&2
  exit 2
fi

env_get() {
  # Read KEY from the env file, stripping quotes; empty string if absent.
  # Semi-sensitive keys (AWS_PROFILE, ACCOUNT_ID, ...) live in .env.local
  # beside the env file, so fall back to it when the key is missing there.
  [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ] || return 0
  local v
  v="$(grep -E "^$1=" "$ENV_FILE" | tail -n1 | cut -d= -f2- | sed 's/^["'\'']//;s/["'\'']$//')"
  local local_file="$(dirname "$ENV_FILE")/.env.local"
  if [ -z "$v" ] && [ -f "$local_file" ]; then
    v="$(grep -E "^$1=" "$local_file" | tail -n1 | cut -d= -f2- | sed 's/^["'\'']//;s/["'\'']$//')"
  fi
  printf '%s\n' "$v"
}

if [ -n "$ENV_FILE" ] && [ ! -f "$ENV_FILE" ]; then
  echo "Error: env file '$ENV_FILE' not found." >&2
  exit 2
fi

STACK="${STACK_ARG:-${STACK:-$(env_get STACK_NAME)}}"
REGION="${REGION_ARG:-${REGION:-$(env_get ACCOUNT_REGION)}}"

# Let the env file pick the AWS profile too; an exported AWS_PROFILE wins.
if [ -z "${AWS_PROFILE:-}" ] && [ -n "$(env_get AWS_PROFILE)" ]; then
  export AWS_PROFILE="$(env_get AWS_PROFILE)"
fi

# No default stack: this repo deploys one stack per collection, so guessing
# which to target would start a backfill against the wrong store.
if [ -z "$STACK" ]; then
  echo "Error: no stack given. Pass -s STACK, set STACK, or use -e ENV_FILE." >&2
  usage
fi

# Timestamped default: unique by construction, so reruns never collide with
# Step Functions' 90-day execution-name uniqueness window.
[ -n "$EXECUTION_NAME" ] || EXECUTION_NAME="${STACK}-backfill-$(date -u +%Y%m%dT%H%M%SZ)"

# Region flag is only added when we actually have one; otherwise defer to the
# AWS CLI's configured default.
REGION_ARGS=()
[ -n "$REGION" ] && REGION_ARGS=(--region "$REGION")

echo "Resolving BackfillStateMachineArn from stack '$STACK'..." >&2
STATE_MACHINE_ARN="$(aws cloudformation describe-stacks \
  "${REGION_ARGS[@]}" \
  --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='BackfillStateMachineArn'].OutputValue" \
  --output text)"

if [ -z "$STATE_MACHINE_ARN" ] || [ "$STATE_MACHINE_ARN" = "None" ]; then
  echo "Error: BackfillStateMachineArn output not found on stack '$STACK'." >&2
  echo "Is BACKFILL_ENABLED=true and the stack deployed in region '${REGION:-<default>}'?" >&2
  exit 1
fi

echo "State machine: $STATE_MACHINE_ARN" >&2
echo "Starting execution '$EXECUTION_NAME'..." >&2

aws stepfunctions start-execution \
  "${REGION_ARGS[@]}" \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --name "$EXECUTION_NAME" \
  --input "{\"inventory_uri\": \"$INVENTORY_URI\"}"

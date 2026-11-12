#!/usr/bin/env bash
# Bundle the job code for the Fargate task (Terraform uploads dist/finops.zip to S3).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p dist && rm -f dist/finops.zip
zip -qr dist/finops.zip dags models config aws generate.py run.py extract_snowflake.py publish_live.py workload.py requirements.txt \
  -x '*/__pycache__/*'
echo dist/finops.zip

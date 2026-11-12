#!/usr/bin/env bash
# Runs inside the Fargate task. JOB=showback (daily) or JOB=workload (weekday team session).
set -euo pipefail
cd /app
python aws/job.py

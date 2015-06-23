#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="${ROOT}/infra/aws-existing-eks"

: "${AWS_REGION:?set AWS_REGION}"
: "${TS_EKS_CLUSTER_NAME:?set TS_EKS_CLUSTER_NAME}"
: "${TS_IMAGE:?set TS_IMAGE}"
export AWS_EC2_METADATA_DISABLED="${AWS_EC2_METADATA_DISABLED:-true}"

cd "${TF_DIR}"

echo "== terraform fmt/check =="
terraform fmt -check

echo "== aws identity =="
aws sts get-caller-identity

echo "== terraform init/validate =="
terraform init -backend=false
terraform validate

echo "== eks cluster =="
aws eks describe-cluster --region "${AWS_REGION}" --name "${TS_EKS_CLUSTER_NAME}" >/tmp/temporalstore-eks-cluster.json

echo "== terraform plan =="
terraform plan \

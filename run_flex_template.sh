#!/usr/bin/env bash

set -e

PROJECT="httparchive"
REPO="data-pipeline"
REGION="us-west1"

# type is the first script argument
TYPE="${1}"
DF_JOB_ID="${REPO}-${TYPE}-$(date -u +%Y%m%d-%H%M%S)"
DF_TEMP_BUCKET="gs://${PROJECT}-staging/dataflow"
TEMPLATE_BASE_PATH="gs://${PROJECT}/dataflow/templates"

# find the latest template if unset
: "${TEMPLATE_PATH:=$(gsutil ls ${TEMPLATE_BASE_PATH}/${REPO}-"${TYPE}"*.json | sort -r | head -n 1)}"

case "${TYPE}~${TEMPLATE_PATH}" in
    all~|combined~) : ;;
    all~gs://*all*) : ;;
    combined~gs://*combined*) : ;;
    tech_report~gs://*tech_report*) : ;;
    *)
        echo "Expected an argumment of either [all|combined|tech_report] and optionally TEMPLATE_PATH to be set (otherwise the latest template will be used)"
        echo "Examples"
        echo "  $(basename "$0") all ..."
        echo "  $(basename "$0") combined ..."
        echo "  $(basename "$0") tech_report ..."
        echo "  TEMPLATE_PATH=${TEMPLATE_BASE_PATH}/${REPO}-all-2022-10-12_00-19-44.json $(basename "$0") all ..."
        echo "  TEMPLATE_PATH=${TEMPLATE_BASE_PATH}/${REPO}-combined-2022-10-12_00-19-44.json $(basename "$0") combined ..."
        echo "  TEMPLATE_PATH=${TEMPLATE_BASE_PATH}/${REPO}-tech_report-2022-10-12_00-19-44.json $(basename "$0") tech_report ..."
        exit 1
        ;;
esac

# drop the first argument
shift

# replace underscores with hyphens in the job id
DF_JOB_ID=${DF_JOB_ID//_/-}

set -u

gcloud dataflow flex-template run "${DF_JOB_ID}" \
    --template-file-gcs-location="${TEMPLATE_PATH}" \
    --staging-location=${DF_TEMP_BUCKET} \
    --region=${REGION} \
    "$@"

#!/usr/bin/env bash

set -e
set -u

BUILD_TAG=$(date -u +"%Y-%m-%d_%H-%M-%S")

# all and combined pipelines
for type in all combined
do
    gcloud builds submit --substitutions=_TYPE="${type}",_BUILD_TAG="${BUILD_TAG}",_WORKER_TYPE=n1-standard-96 .
done

# tech_report pipeline
gcloud builds submit --substitutions=_TYPE=tech_report,_BUILD_TAG="${BUILD_TAG}",_WORKER_TYPE=n1-standard-1 .

echo "${BUILD_TAG}"

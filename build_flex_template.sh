#!/usr/bin/env bash

set -e
set -u

BUILD_TAG=$(date -u +"%Y-%m-%d_%H-%M-%S")

gcloud builds submit --substitutions=_TYPE="all",_BUILD_TAG="${BUILD_TAG}" .

# Sleep to ensure the previous command completes as in GitHub actions it
# tries to run the builds in parallel which fails.
sleep 600

gcloud builds submit --substitutions=_TYPE="combined",_BUILD_TAG="${BUILD_TAG}" .

echo "${BUILD_TAG}"

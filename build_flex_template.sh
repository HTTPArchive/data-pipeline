#!/usr/bin/env bash

set -e
set -u

BUILD_TAG=$(date -u +"%Y-%m-%d_%H-%M-%S")

gcloud builds submit --substitutions=_TYPE="combined",_BUILD_TAG="${BUILD_TAG}" .

echo "${BUILD_TAG}"

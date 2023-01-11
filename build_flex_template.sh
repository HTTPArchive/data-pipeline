#!/usr/bin/env bash

set -e
set -u

BUILD_TAG=$(date +"%Y-%m-%d_%H-%M-%S")

for type in all combined
do
    gcloud builds submit --substitutions=_TYPE="${type}",_BUILD_TAG="${BUILD_TAG}" .
done
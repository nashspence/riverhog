#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_compose_env.sh"

setup_test_compose_project
configure_compose_tty
export COMPOSE_PROFILES=development
ensure_compose_image test

export RIVERHOG_S3_FIXTURE_ACCESS_KEY_ID="$(compose_env_value RIVERHOG_S3_FIXTURE_ACCESS_KEY_ID riverhog-test-key)"
export RIVERHOG_S3_FIXTURE_SECRET_ACCESS_KEY="$(compose_env_value RIVERHOG_S3_FIXTURE_SECRET_ACCESS_KEY riverhog-test-secret)"
export RIVERHOG_S3_FIXTURE_ENDPOINT_URL="$(compose_env_value RIVERHOG_S3_FIXTURE_ENDPOINT_URL http://s3-fixture:9000)"
export RIVERHOG_S3_FIXTURE_REGION="$(compose_env_value RIVERHOG_S3_FIXTURE_REGION us-east-1)"
archive_bucket="$(compose_env_value RIVERHOG_S3_FIXTURE_ARCHIVE_BUCKET riverhog-archive)"
cache_bucket="$(compose_env_value RIVERHOG_S3_FIXTURE_CACHE_BUCKET riverhog-cache)"

compose up --detach s3-fixture

provision_bucket() {
  export RIVERHOG_S3_FIXTURE_PROVISION_BUCKET="$1"
  shift
  compose run --rm -T --no-deps --entrypoint python \
    -e RIVERHOG_S3_FIXTURE_PROVISION_BUCKET \
    -e RIVERHOG_S3_FIXTURE_ACCESS_KEY_ID \
    -e RIVERHOG_S3_FIXTURE_SECRET_ACCESS_KEY \
    -e RIVERHOG_S3_FIXTURE_ENDPOINT_URL \
    -e RIVERHOG_S3_FIXTURE_REGION \
    test -m tests.harness.provision_s3_fixture "$@"
}
provision_bucket "${archive_bucket}" --verify-conditional-writes
if [[ "${cache_bucket}" != "${archive_bucket}" ]]; then
  provision_bucket "${cache_bucket}"
fi

SHELL := bash
.DEFAULT_GOAL := help

MISE_BIN ?= mise
FILES ?= .
TESTS ?= packages some-implementations riverhog tests/unit
UNIT_PYTEST_ARGS ?= -n 4 --dist=loadscope --instafail --durations=30 --durations-min=0.25
PYTHON_PATHS ?= packages some-implementations riverhog scripts tests
RELEASE_VERSION ?= 1.0.0
RELEASE_OUTPUT ?=
RELEASE_SUMMARY ?=
RELEASE_SIGNING_KEY ?=
RELEASE_PUBLIC_KEY ?=
UV_RUN = "$(MISE_BIN)" x -- uv run --locked --all-packages --group dev
BAKE_FILE = docker-bake.hcl
args ?=

.PHONY: help license ruff ruff-fix format format-check fix mypy lint compile unit dependency-readiness operation-qualification database-qualification contract-freeze contract-freeze-update contract-browser contract-browser-docker profile provider-qualification installation-qualification release-check release-plan release-dry-run release-governance-check release-evidence release-verify c2sp-vectors postgres-concurrency compose-smoke filesystem-recovery-qualification stove0-scale-qualification a-riverhog-event-relay-smoke dist dist-smoke build bootstrap-garage down test

define UV_CMD
	@if ! command -v "$(MISE_BIN)" >/dev/null 2>&1; then \
		printf '%s\n' 'Riverhog Makefile targets require mise on PATH, or MISE_BIN=/abs/path/to/mise.' >&2; \
		printf '%s\n' 'Install mise or pass MISE_BIN explicitly, then rerun make.' >&2; \
		exit 127; \
	fi; \
	$(if $(2),$(2) )$(UV_RUN) $(1)
endef

define BAKE_IMAGE
	@revision="$$(git rev-parse --verify HEAD)"; \
	created="$$(git show -s --format=%cI HEAD)"; \
	docker buildx bake --file "$(BAKE_FILE)" --load \
		--set "*.args.SOURCE_REVISION=$$revision" \
		--set "*.args.BUILD_CREATED=$$created" \
		--set "*.args.SOURCE_DATE_EPOCH=0" \
		--set "*.args.RELEASE_VERSION=development" "$(1)"
endef

help:
	@printf '%s\n' \
		'Targets:' \
		'  make license           Verify SPDX/REUSE coverage for every tracked path.' \
		'  make ruff              Run repo-wide ruff in the locked local uv environment.' \
		'  make ruff-fix          Run ruff --fix in the locked local uv environment.' \
		'  make format            Run ruff format in the locked local uv environment.' \
		'  make format-check      Verify ruff formatting without changing files.' \
		'  make fix               Run ruff-fix, then format.' \
		'  make mypy              Run repo-wide mypy in the locked local uv environment.' \
		'  make lint              Run license, format, ruff, and mypy checks.' \
		'  make compile           Byte-compile all repository Python files.' \
		'  make unit              Run the unit test lane locally.' \
		'  make dependency-readiness Verify the live uv graph and Dependabot release gate.' \
		'  make operation-qualification Verify or emit the generated operation matrix.' \
		'  make database-qualification Record exact-SHA database scale evidence.' \
		'  make contract-freeze   Verify the checked-in v1 boundary and external contract.' \
		'  make contract-freeze-update Regenerate that contract for semantic review.' \
		'  make contract-browser  Exercise the checked candidate in Chromium (run MISE_EXPERIMENTAL=1 mise bootstrap --yes, then Playwright install --only-shell chromium).' \
		'  make contract-browser-docker Run the Chromium checks in a disposable official Playwright container.' \
		'  make profile           Report target, observed transfer or recovery work, and measured comparison without gating.' \
		'  make provider-qualification Run the operator/provider qualification command.' \
		'  make installation-qualification Stage and qualify independent uv-tool installs.' \
		'  make release-check     Validate the coordinated release-unit contract.' \
		'  make release-plan      Print the exact-SHA v1 release inventory as JSON.' \
		'  make release-dry-run   Version and smoke-test an exact-SHA copy without publishing.' \
		'  make release-governance-check Verify live GitHub controls against release.toml.' \
		'  make release-evidence  Build signed exact-SHA evidence with external release keys.' \
		'  make release-verify    Verify a generated release evidence directory.' \
		'  make c2sp-vectors      Download and run the pinned C2SP age conformance corpus.' \
		'  make postgres-concurrency Run database concurrency tests against disposable Postgres.' \
		'  make compose-smoke     Verify disposable adapter, Riverhog, cache, and stove0 lifecycle.' \
		'  make filesystem-recovery-qualification Prove built-service recovery from filesystem storage.' \
		'  make stove0-scale-qualification Run the final-image lifecycle with a 128-file workload.' \
		'  make a-riverhog-event-relay-smoke  Exercise the already-built final Riverhog event relay image.' \
		'  make dist              Build every Python distribution independently.' \
		'  make dist-smoke        Install and exercise the Riverhog server and client wheels.' \
		'  make build-<bake-target> Build one target declared in docker-bake.hcl.' \
		'  make build             Build the docker-bake.hcl default image group.' \
		'  make bootstrap-garage  Start Garage and apply the checked-in bucket/key bootstrap.' \
		'  make down              Tear the compose-managed test stack down.' \
		'  make test              Run lint, then unit.' \
		'' \
		'Variables:' \
		"  args='...'             Forward arguments to ruff, mypy, or pytest lanes." \
		"  FILES='...'            Narrow ruff and format targets to specific files." \
		"  PYTHON_PATHS='...'      Narrow the Python compile lane." \
		"  TESTS='...'            Narrow the unit test lane to specific tests." \
		"  POSTGRES_TESTS='...'   Select disposable Postgres test files." \
		'  STOVE0_SCALE_FILES=N  Set the scale-qualification file count (default: 128).' \
		'  STOVE0_SCALE_AUDIO_FRAMES=N Set frames per scale fixture (default: 2000).' \
		'  RELEASE_VERSION=1.0.0 Coordinated version for release-plan and release-dry-run.' \
		'  RELEASE_OUTPUT=/path   Output/evidence directory for release-evidence or release-verify.' \
		'  RELEASE_SUMMARY=/path  Write a JSON dry-run or governance summary.' \
		'  RELEASE_SIGNING_KEY=/path Offline minisign secret key for release-evidence.' \
		'  RELEASE_PUBLIC_KEY=/path Minisign public key for release-evidence or release-verify.' \
		'  MISE_BIN=/abs/path/to/mise Use a specific mise binary instead of mise on PATH.' \
		'  COMPOSE_ENV_FILE=/abs/path/to/overrides.env' \
		'  TEST_COMPOSE_PROJECT_NAME=riverhog-shared'

license:
	$(call UV_CMD,python -m reuse lint)

ruff:
	$(call UV_CMD,python -m ruff check $(FILES) $(args))

ruff-fix:
	$(call UV_CMD,python -m ruff check --fix $(FILES) $(args))

format:
	$(call UV_CMD,python -m ruff format $(FILES) $(args))

format-check:
	$(call UV_CMD,python -m ruff format --check $(FILES) $(args))

fix: ruff-fix format

mypy:
	$(call UV_CMD,python scripts/run_mypy.py $(args))

lint: license format-check ruff mypy

compile:
	$(call UV_CMD,python -m compileall -q $(PYTHON_PATHS))

unit:
	$(call UV_CMD,python -m pytest -q $(UNIT_PYTEST_ARGS) $(TESTS) $(args))

dependency-readiness:
	$(call UV_CMD,python scripts/check_dependency_readiness.py $(args))

operation-qualification:
	$(call UV_CMD,python scripts/operation_qualification.py $(if $(strip $(args)),$(args),check))

database-qualification:
	@DATABASE_QUALIFICATION_OUTPUT="$(DATABASE_QUALIFICATION_OUTPUT)" \
		DATABASE_QUALIFICATION_SOURCE_SHA="$(DATABASE_QUALIFICATION_SOURCE_SHA)" \
		./scripts/test_database_qualification.sh

contract-freeze:
	$(call UV_CMD,python scripts/contract_freeze.py check)

contract-freeze-update:
	$(call UV_CMD,python scripts/contract_freeze.py update)

contract-browser:
	"$(MISE_BIN)" x -- uv run --locked --all-packages --group dev --group browser \
		python -m pytest -q tests/browser

contract-browser-docker:
	MISE_BIN="$(MISE_BIN)" ./scripts/test_contract_browser_docker.sh

profile:
	$(call UV_CMD,python scripts/transfer_profile.py $(args))

provider-qualification:
	$(call UV_CMD,python scripts/provider_qualification.py $(args))

installation-qualification:
	$(call UV_CMD,python scripts/qualify_installation.py --version "$(RELEASE_VERSION)" $(args))

release-check:
	$(call UV_CMD,python scripts/release.py check)

release-plan:
	$(call UV_CMD,python scripts/release.py plan --version "$(RELEASE_VERSION)" $(args))

release-dry-run:
	$(call UV_CMD,python scripts/release.py dry-run --version "$(RELEASE_VERSION)" $(if $(RELEASE_SUMMARY),--summary "$(RELEASE_SUMMARY)"))

release-governance-check:
	$(call UV_CMD,python scripts/github_governance.py check $(if $(RELEASE_GOVERNANCE_SCOPE),--scope "$(RELEASE_GOVERNANCE_SCOPE)") $(if $(RELEASE_SUMMARY),--summary "$(RELEASE_SUMMARY)"))

release-evidence:
	@if [[ -z "$(RELEASE_OUTPUT)" || -z "$(RELEASE_SIGNING_KEY)" || -z "$(RELEASE_PUBLIC_KEY)" ]]; then \
		printf '%s\n' 'RELEASE_OUTPUT, RELEASE_SIGNING_KEY, and RELEASE_PUBLIC_KEY are required.' >&2; \
		exit 2; \
	fi
	$(call UV_CMD,python scripts/release.py evidence --version "$(RELEASE_VERSION)" --output "$(RELEASE_OUTPUT)" --signing-key "$(RELEASE_SIGNING_KEY)" --public-key "$(RELEASE_PUBLIC_KEY)")

release-verify:
	@if [[ -z "$(RELEASE_OUTPUT)" || -z "$(RELEASE_PUBLIC_KEY)" ]]; then \
		printf '%s\n' 'RELEASE_OUTPUT and RELEASE_PUBLIC_KEY are required.' >&2; \
		exit 2; \
	fi
	$(call UV_CMD,python scripts/release.py verify --directory "$(RELEASE_OUTPUT)" --public-key "$(RELEASE_PUBLIC_KEY)")

c2sp-vectors:
	@MISE_BIN="$(MISE_BIN)" ./scripts/test_c2sp_vectors.sh

postgres-concurrency:
	@POSTGRES_TESTS="$(POSTGRES_TESTS)" ./scripts/test_postgres_concurrency.sh

compose-smoke:
	@./scripts/test_compose_smoke.sh

filesystem-recovery-qualification:
	@./scripts/test_filesystem_recovery_qualification.sh

stove0-scale-qualification:
	@STOVE0_SMOKE_FILE_COUNT="$${STOVE0_SCALE_FILES:-128}" \
		STOVE0_SMOKE_AUDIO_FRAMES="$${STOVE0_SCALE_AUDIO_FRAMES:-2000}" \
		./scripts/test_compose_smoke.sh

dist:
	@if ! command -v "$(MISE_BIN)" >/dev/null 2>&1; then \
		printf '%s\n' 'Riverhog Makefile targets require mise on PATH, or MISE_BIN=/abs/path/to/mise.' >&2; \
		exit 127; \
	fi
	@"$(MISE_BIN)" x -- uv build --all-packages --clear --no-create-gitignore
	@$(UV_RUN) python scripts/check_distribution_licenses.py dist

dist-smoke: dist
	@$(UV_RUN) python scripts/release.py verify-distributions
	@MISE_BIN="$(MISE_BIN)" ./scripts/test_distributions.sh

.PHONY: FORCE
FORCE:

build-%: FORCE
	$(call BAKE_IMAGE,$*)

a-riverhog-event-relay-smoke:
	@if ! command -v "$(MISE_BIN)" >/dev/null 2>&1; then \
		printf '%s\n' 'Riverhog event relay image smoke requires mise on PATH, or MISE_BIN=/abs/path/to/mise.' >&2; \
		exit 127; \
	fi
	@"$(MISE_BIN)" x python -- python scripts/test_a_riverhog_event_relay_image.py

build:
	$(call BAKE_IMAGE,default)

bootstrap-garage:
	@./scripts/bootstrap_garage.sh

down:
	@./scripts/compose_down.sh

test: lint unit

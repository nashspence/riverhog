FROM jdxcode/mise:2026.7.13@sha256:7fe9145156e33f95f7712632dbadb418592ed0f60fb46e5bcc8c113d372ad8a3 AS mise

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS locked-tools

COPY --from=mise /usr/local/bin/mise /usr/local/bin/mise
WORKDIR /workspace
COPY mise.toml mise.lock ./
RUN mise trust mise.toml \
    && mise install --locked uv \
    && mkdir -p /opt/riverhog-tools/bin \
    && cp --preserve=mode,timestamps "$(mise which uv)" /opt/riverhog-tools/bin/uv \
    && test "$(/opt/riverhog-tools/bin/uv --version)" = "$(mise exec uv -- uv --version)"

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS build

ARG SOURCE_DATE_EPOCH=0
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}

COPY --from=locked-tools /opt/riverhog-tools/bin/uv /usr/local/bin/uv
WORKDIR /workspace
COPY pyproject.toml uv.lock ./
COPY some-implementations/stove0/targets/opus/target some-implementations/stove0/targets/opus/target
COPY some-implementations/stove0/review0/samplers/opus some-implementations/stove0/review0/samplers/opus
COPY packages/riverhog-application-access packages/riverhog-application-access
COPY packages/http-api-contracts packages/http-api-contracts
COPY packages/riverhog-canonical-json packages/riverhog-canonical-json
COPY packages/lifecycle-events packages/lifecycle-events
COPY packages/riverhog-client packages/riverhog-client
COPY packages/riverhog-protocol packages/riverhog-protocol
COPY packages/riverhog-provenance-contracts packages/riverhog-provenance-contracts
COPY some-implementations/stove0/targets/media-archive/contracts some-implementations/stove0/targets/media-archive/contracts
COPY some-implementations/stove0/targets/media-archive/support some-implementations/stove0/targets/media-archive/support
COPY some-implementations/stove0/observers/contracts/media-metadata some-implementations/stove0/observers/contracts/media-metadata
COPY some-implementations/stove0/packages/observer-protocol some-implementations/stove0/packages/observer-protocol
COPY some-implementations/stove0/packages/protocol some-implementations/stove0/packages/protocol
COPY some-implementations/stove0/review0/contracts some-implementations/stove0/review0/contracts
COPY some-implementations/stove0/review0/sampler/client some-implementations/stove0/review0/sampler/client
COPY some-implementations/stove0/review0/sampler/protocol some-implementations/stove0/review0/sampler/protocol
COPY some-implementations/stove0/review0/sampler/support some-implementations/stove0/review0/sampler/support
COPY some-implementations/stove0/packages/target-client some-implementations/stove0/packages/target-client
COPY some-implementations/stove0/packages/target-protocol some-implementations/stove0/packages/target-protocol
COPY some-implementations/stove0/packages/target-support some-implementations/stove0/packages/target-support
COPY packages/time-formats packages/time-formats
RUN uv sync --frozen --package a-stove0-opus-target --package a-review0-opus-sampler --no-dev --no-editable

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203

ARG SOURCE_DATE_EPOCH=0
ENV DEBIAN_FRONTEND=noninteractive \
    PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg tini \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o 65532 -g 65532 -m 0700 /var/lib/a-stove0-opus-target \
    && ! command -v mise

COPY --from=build /opt/venv /opt/venv

COPY LICENSES/Apache-2.0.txt /usr/share/licenses/riverhog/Apache-2.0.txt
COPY LICENSES/CAL-1.0.txt /usr/share/licenses/riverhog/CAL-1.0.txt
COPY THIRD_PARTY_NOTICES.md /usr/share/doc/riverhog/THIRD_PARTY_NOTICES.md

ARG BUILD_CREATED=1970-01-01T00:00:00Z
ARG RELEASE_VERSION=development
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.title="stove0 Opus target" \
      org.opencontainers.image.description="Opus transformation target for Stove0." \
      org.opencontainers.image.licenses="CAL-1.0" \
      org.opencontainers.image.source="https://github.com/nashspence/riverhog" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.version="${RELEASE_VERSION}" \
      org.opencontainers.image.created="${BUILD_CREATED}" \
      io.github.nashspence.riverhog.release-role="component" \
      org.opencontainers.image.documentation="https://nashspence.github.io/riverhog/v1/"


USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["a-stove0-opus-target", "--host", "0.0.0.0", "--port", "8080"]

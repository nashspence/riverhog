FROM jdxcode/mise:2026.7.13@sha256:7fe9145156e33f95f7712632dbadb418592ed0f60fb46e5bcc8c113d372ad8a3 AS mise

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS locked-tools

COPY --from=mise /usr/local/bin/mise /usr/local/bin/mise
WORKDIR /workspace
COPY mise.toml mise.lock ./
RUN mise trust mise.toml \
    && mise install --locked uv rclone \
    && rclone_path="$(mise which rclone)" \
    && mkdir -p /opt/riverhog-tools/bin /opt/riverhog-tools/licenses/rclone/1.75.0 \
    && cp --preserve=mode,timestamps "$(mise which uv)" /opt/riverhog-tools/bin/uv \
    && cp --preserve=mode,timestamps "${rclone_path}" /opt/riverhog-tools/bin/rclone \
    && awk 'seen { if ($0 == "Authors and contributors") exit; print } $0 == "License" { seen=1; print }' \
        "$(dirname "${rclone_path}")/README.txt" \
        > /opt/riverhog-tools/licenses/rclone/1.75.0/LICENSE \
    && echo "f47e4137bcc0bf4554ad5f9e3dd361c738fb95e27c24f5d0792bc2479bd241b6  /opt/riverhog-tools/licenses/rclone/1.75.0/LICENSE" \
        | sha256sum -c - \
    && test "$(/opt/riverhog-tools/bin/uv --version)" = "$(mise exec uv -- uv --version)" \
    && /opt/riverhog-tools/bin/rclone version

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS build

ARG SOURCE_DATE_EPOCH=0
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}

COPY --from=locked-tools /opt/riverhog-tools/bin/uv /usr/local/bin/uv
WORKDIR /workspace
COPY pyproject.toml uv.lock ./
COPY packages/config-validation packages/config-validation
COPY some-implementations/stove0/review0/rclone-effect-target some-implementations/stove0/review0/rclone-effect-target
COPY some-implementations/stove0/review0/support some-implementations/stove0/review0/support
COPY packages/riverhog-application-access packages/riverhog-application-access
COPY packages/http-api-contracts packages/http-api-contracts
COPY packages/riverhog-canonical-json packages/riverhog-canonical-json
COPY packages/lifecycle-events packages/lifecycle-events
COPY packages/riverhog-client packages/riverhog-client
COPY packages/riverhog-protocol packages/riverhog-protocol
COPY packages/riverhog-provenance-contracts packages/riverhog-provenance-contracts
COPY some-implementations/stove0/packages/protocol some-implementations/stove0/packages/protocol
COPY some-implementations/stove0/review0/contracts some-implementations/stove0/review0/contracts
COPY some-implementations/stove0/review0/sampler/client some-implementations/stove0/review0/sampler/client
COPY some-implementations/stove0/review0/sampler/protocol some-implementations/stove0/review0/sampler/protocol
COPY some-implementations/stove0/packages/target-client some-implementations/stove0/packages/target-client
COPY some-implementations/stove0/packages/target-protocol some-implementations/stove0/packages/target-protocol
COPY some-implementations/stove0/packages/target-support some-implementations/stove0/packages/target-support
COPY packages/time-formats packages/time-formats
RUN uv sync --frozen --package a-review0-rclone-target --no-dev --no-editable

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203

ENV DEBIAN_FRONTEND=noninteractive \
    PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ARG BUILD_CREATED=1970-01-01T00:00:00Z
ARG RELEASE_VERSION=development
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.title="Review0 rclone target" \
      org.opencontainers.image.description="Review0 rclone delivery target for Stove0." \
      org.opencontainers.image.licenses="CAL-1.0" \
      org.opencontainers.image.source="https://github.com/nashspence/riverhog" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.version="${RELEASE_VERSION}" \
      org.opencontainers.image.created="${BUILD_CREATED}" \
      io.github.nashspence.riverhog.release-role="component" \
      org.opencontainers.image.documentation="https://nashspence.github.io/riverhog/v1/"

COPY LICENSES/Apache-2.0.txt /usr/share/licenses/riverhog/Apache-2.0.txt
COPY LICENSES/CAL-1.0.txt /usr/share/licenses/riverhog/CAL-1.0.txt
COPY THIRD_PARTY_NOTICES.md /usr/share/doc/riverhog/THIRD_PARTY_NOTICES.md
COPY --from=locked-tools /opt/riverhog-tools/licenses/rclone/1.75.0/LICENSE /usr/share/licenses/riverhog-third-party/rclone/1.75.0/LICENSE

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o 65532 -g 65532 -m 0700 \
        /var/lib/a-review0-rclone-target \
        /var/lib/review0-delivery

COPY --from=build /opt/venv /opt/venv
COPY --from=locked-tools /opt/riverhog-tools/bin/rclone /usr/local/bin/rclone

RUN rclone version \
    && ! command -v mise \
    && ! command -v ffmpeg

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["a-review0-rclone-target", "--host", "0.0.0.0", "--port", "8080"]

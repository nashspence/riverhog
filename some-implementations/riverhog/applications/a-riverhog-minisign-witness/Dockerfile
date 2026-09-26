FROM jdxcode/mise:2026.7.13@sha256:7fe9145156e33f95f7712632dbadb418592ed0f60fb46e5bcc8c113d372ad8a3 AS mise

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS locked-tools
COPY --from=mise /usr/local/bin/mise /usr/local/bin/mise
WORKDIR /workspace
COPY mise.toml mise.lock ./
COPY third_party/minisign/0.12/LICENSE /tmp/minisign-LICENSE
RUN mise trust mise.toml \
    && mise install --locked uv minisign \
    && mkdir -p /opt/riverhog-tools/bin /opt/riverhog-tools/licenses/minisign/0.12 \
    && cp --preserve=mode,timestamps "$(mise which uv)" /opt/riverhog-tools/bin/uv \
    && cp --preserve=mode,timestamps "$(mise which minisign)" /opt/riverhog-tools/bin/minisign \
    && cp /tmp/minisign-LICENSE /opt/riverhog-tools/licenses/minisign/0.12/LICENSE \
    && chmod 0644 /opt/riverhog-tools/licenses/minisign/0.12/LICENSE \
    && echo "01aa50818d6609e85a9610b7d04da037e8b28bcf3bb5b5b4963c0c3f7abdabbd  /opt/riverhog-tools/licenses/minisign/0.12/LICENSE" | sha256sum -c - \
    && test "$(/opt/riverhog-tools/bin/uv --version)" = "$(mise exec uv -- uv --version)" \
    && /opt/riverhog-tools/bin/minisign -v

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS build
ARG SOURCE_DATE_EPOCH=0
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}
COPY --from=locked-tools /opt/riverhog-tools/bin/uv /usr/local/bin/uv
WORKDIR /workspace
COPY pyproject.toml uv.lock ./
COPY some-implementations/riverhog/applications/a-riverhog-minisign-witness some-implementations/riverhog/applications/a-riverhog-minisign-witness
COPY packages/riverhog-application-access packages/riverhog-application-access
COPY packages/http-api-contracts packages/http-api-contracts
COPY packages/lifecycle-events packages/lifecycle-events
COPY packages/riverhog-canonical-json packages/riverhog-canonical-json
COPY packages/riverhog-client packages/riverhog-client
COPY packages/riverhog-protocol packages/riverhog-protocol
COPY packages/riverhog-provenance-contracts packages/riverhog-provenance-contracts
COPY packages/state-schema packages/state-schema
COPY packages/time-formats packages/time-formats
COPY some-implementations/riverhog/packages/a-riverhog-witness-contract-lib some-implementations/riverhog/packages/a-riverhog-witness-contract-lib
RUN uv sync --frozen --package a-riverhog-minisign-witness --no-dev --no-editable

FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
COPY LICENSES/Apache-2.0.txt /usr/share/licenses/riverhog/Apache-2.0.txt
COPY LICENSES/CAL-1.0.txt /usr/share/licenses/riverhog/CAL-1.0.txt
COPY THIRD_PARTY_NOTICES.md /usr/share/doc/riverhog/THIRD_PARTY_NOTICES.md
COPY --from=locked-tools /opt/riverhog-tools/licenses/minisign/0.12/LICENSE /usr/share/licenses/riverhog-third-party/minisign/0.12/LICENSE
RUN install -d -o 65532 -g 65532 -m 0700 /state && ! command -v mise
COPY --from=build /opt/venv /opt/venv
COPY --from=locked-tools /opt/riverhog-tools/bin/minisign /usr/local/bin/minisign
RUN minisign -v

ARG BUILD_CREATED=1970-01-01T00:00:00Z
ARG RELEASE_VERSION=development
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.title="Riverhog Minisign collection witness" \
      org.opencontainers.image.description="Independent Minisign collection witness for Riverhog." \
      org.opencontainers.image.licenses="CAL-1.0" \
      org.opencontainers.image.source="https://github.com/nashspence/riverhog" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.version="${RELEASE_VERSION}" \
      org.opencontainers.image.created="${BUILD_CREATED}" \
      io.github.nashspence.riverhog.release-role="application" \
      org.opencontainers.image.documentation="https://nashspence.github.io/riverhog/v1/"

USER 65532:65532
ENTRYPOINT ["a-riverhog-minisign-witness"]

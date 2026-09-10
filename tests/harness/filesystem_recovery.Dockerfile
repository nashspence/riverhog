FROM riverhog-test:dev AS wheels

RUN uv build --all-packages --clear --no-create-gitignore --out-dir /wheels \
    && uv export --frozen --all-packages --no-dev --no-emit-workspace --no-hashes \
      --output-file /constraints.txt


FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS client

ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      io.github.nashspence.riverhog.release-role="qualification"

COPY --from=wheels /usr/local/bin/uv /usr/local/bin/uv
COPY --from=wheels /wheels /wheels
COPY --from=wheels /constraints.txt /constraints.txt
RUN uv venv /opt/venv \
    && uv pip install --strict --python /opt/venv/bin/python --find-links /wheels \
      --constraints /constraints.txt \
      piggity==0.1.0 riverhog-provenance-linux-observer==0.1.0 \
    && /opt/venv/bin/python -I -c \
      'import importlib.util; assert importlib.util.find_spec("riverhog_core") is None'

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 65532:65532
ENTRYPOINT ["piggity"]


FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS recovery

ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.revision="${SOURCE_REVISION}" \
      io.github.nashspence.riverhog.release-role="qualification"

COPY --from=wheels /usr/local/bin/uv /usr/local/bin/uv
COPY --from=wheels /usr/local/bin/age /usr/local/bin/age
COPY --from=wheels /usr/local/bin/age-plugin-batchpass /usr/local/bin/age-plugin-batchpass
COPY --from=wheels /wheels /wheels
COPY --from=wheels /constraints.txt /constraints.txt
RUN uv venv /opt/venv \
    && uv pip install --strict --python /opt/venv/bin/python --find-links /wheels \
      --constraints /constraints.txt \
      riverhog-recover==0.1.0 \
    && test "$(age --version)" = "v1.3.1" \
    && test "$(age-plugin-batchpass --version)" = "v1.3.1" \
    && /opt/venv/bin/python -I -c \
      'import importlib.util; assert importlib.util.find_spec("riverhog_core") is None; assert importlib.util.find_spec("sqlalchemy") is None'

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 65532:65532
ENTRYPOINT ["riverhog-recover"]

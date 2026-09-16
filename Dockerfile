FROM public.ecr.aws/docker/library/python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea AS base

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/field-sessions-parser
COPY requirements.lock ./
RUN pip install --no-cache-dir --require-hashes -r requirements.lock

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps --no-build-isolation .

ARG FIELD_SESSIONS_PARSER_VERSION
RUN test -n "$FIELD_SESSIONS_PARSER_VERSION"
ENV FIELD_SESSIONS_PARSER_VERSION=$FIELD_SESSIONS_PARSER_VERSION

RUN mkdir -p /tmp/resim/outputs

ENTRYPOINT ["field-sessions-parser"]

FROM base AS test
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY tests ./tests
COPY scripts ./scripts
RUN pip install --no-cache-dir pytest==9.1.1 ruff==0.16.7
RUN pytest -q && ruff check . && ruff format --check .

FROM base AS parser

FROM public.ecr.aws/docker/library/busybox:stable@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662 AS noop

ENTRYPOINT ["true"]

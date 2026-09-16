# Field Sessions Parser

MCAP readers and exact-timestamp Emitter helpers for customer-owned ReSim metrics builds. Python 3.12 is required. The library supports JSON, ROS 1, ROS 2 message/IDL and protobuf encodings inside MCAP, including local files and bounded S3/HTTPS range reads. ROS bag containers, text logs, Parquet and HDF5 are not supported.

## Install and inspect

Download the wheel and `requirements.lock` from a [release](https://github.com/resim-ai/field-sessions-parser/releases). This release lock is the native reader lock; it does not install the Linux SDK. In a Python 3.12 virtual environment:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements.lock
.venv/bin/python -m pip install --no-deps field_sessions_parser-0.2.1-py3-none-any.whl
.venv/bin/field-sessions-parser inspect recording.mcap
```

The [Signal Flag plugin](https://github.com/resim-ai/signal-flag-plugin) provides an isolated setup command and bundled reader for Claude users; no source checkout is needed to use that bundle. This repository does not currently publish to PyPI.

```python
from field_sessions_parser import open

recording = open("recording.mcap")
for record in recording.iter_messages():
    print(record.topic, record.timestamp_ns, record.recording_key)
```

`open`, `iter_messages` and `inspect` preserve integer epoch nanoseconds and relative recording provenance. Inspection traverses the complete recording; it does not infer analysis logic. Unsupported decoding, corrupt input and incomplete reads fail visibly.

## Customer metrics builds

The optional `sdk` extra pins `resim-open-core==1.5.0` for the supported Linux metrics-build runtime. The root Dockerfile's `parser` target includes this SDK; its `noop` target supplies the separate no-op execution image. Pin published image digests in customer Dockerfiles.

[The editable customer example](tests/fixtures/customer_repo/README.md) contains `job.py`, ordinary `config.resim.yml`, a pinned Dockerfile and synthetic tests. Its default inventory profile counts recorded messages and captures recording bounds without robot-specific claims. The explicit Burro profile reports recorded state flags and GPS changes without inferred failure thresholds. Customer code owns signal selection and analysis; this package has no parse-plan interpreter.

Jobs read `/tmp/resim/inputs/experience` and write `/tmp/resim/outputs/emissions.resim.jsonl`. The `emissions.TOPICS` schemas describe `session_inventory` and `session_events`; declare the latter as `event: true`. Emitter helpers preserve event time and recording provenance in reserved event tags. Complete input validation must succeed before emissions are committed. The baked config is an initial authoring input; registered branch configs may subsequently select compatible SQL metrics over those emissions.

## Develop and test

From this repository root, use Python 3.12:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install --require-hashes -r requirements-readers.lock
.venv/bin/python -m pip install '.[dev]'
.venv/bin/python -m pytest -m 'not sdk' -q
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
```

The native tests do not require the SDK, AWS credentials or customer recordings. The Linux Docker `test` target runs the entire suite, including real SDK emissions and the customer job entrypoint:

```bash
docker build --target test --build-arg FIELD_SESSIONS_PARSER_VERSION=local-test .
```

The root `requirements.lock` includes SDK and build dependencies; the native `requirements-readers.lock` excludes them. Regenerate locks with `uv pip compile` using the exact command recorded at each file's top. Keep pins stable unless intentionally updating a dependency. Do not replace the Docker lock with the native lock.

## Bundle and release

Build a portable plugin bundle from a committed revision using `uv==0.9.26` and an installed Python 3.12 build interpreter:

```bash
python3 scripts/vendor_reader.py --revision <full-commit-sha> --output /tmp/reader-bundle
```

The script reads a Git archive, excludes uncommitted files, checks build-dependency hashes and fixes the wheel source timestamp. The manifest records the wheel and lock SHA-256 hashes, public source repository and full source revision. A pure-Python wheel still depends on compatible native numerical/compression wheels; actual platform tests determine support.

The package workflow tests Linux and macOS with Python 3.12, runs the Linux SDK Docker target, and builds a bundle from the exact commit. A `v<package-version>` tag publishes those artifacts as a GitHub release, with GitHub's source archives. It refuses to overwrite an existing release. Image publication is separate: the reusable `publish-images.yml` workflow is called by ReSim's backend repository with an immutable source SHA and its existing AWS identity. It does not grant this repository an AWS role.

Version 0.2.1 moves the package from ReSim's backend repository without changing reader behavior. Version 0.2.0 removed non-MCAP adapters. Existing published wheels and images remain immutable.

## Remote HTTP reads

HTTPS sources must honor byte ranges with matching `206` Content-Range and body length. Truncated responses are discarded and retried through the same transport within the configured attempt limit. A `200` response that ignores Range is rejected before its body is read; the reader does not substitute a full-object download. Known size and ETag changes fail. Without a strong initial ETag, response validation does not establish immutable object identity. Persistent truncation and unsupported ranges remain explicit failures.

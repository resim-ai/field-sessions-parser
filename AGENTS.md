# Field Sessions Parser

This repository owns `field-sessions-parser`, an MCAP reader and Emitter helper package. Python 3.12 is the supported runtime. `src/field_sessions_parser/` contains decoders, local/remote readers, emissions and the inspection CLI. Preserve exact epoch nanoseconds, relative recording keys, bounded HTTP reads and fail-closed schema validation. ROS definitions sharing a type must agree on constants and serialized fields. Do not add non-MCAP adapters without an actual customer input requirement.

`tests/` contains generated synthetic recordings and behavioral regressions. `tests/fixtures/customer_repo/` is an editable inventory/Burro metrics-build example, not a customer's robot repository; follow its local AGENTS.md before editing it. No real recordings, credentials or host evidence belong here. Never run the example against customer code as a test.

Use `python -m pytest -m 'not sdk' -q`, `ruff check .` and `ruff format --check .` in the Python 3.12 development environment described in README.md. The root Dockerfile `test` target runs all tests with the real Linux SDK. Native tests and SDK tests provide different evidence; do not claim the latter from native mocks. `test_vendor_reader.py` verifies committed-source isolation and bundle failures using actual Git and a controlled build subprocess; actual wheel builds are also required.

`requirements-readers.lock` is the portable native lock; `requirements.lock` pins SDK and build dependencies for Docker. Their headers contain regeneration commands. Keep `pyproject.toml` and `src/field_sessions_parser/__init__.py` versions aligned. SDK remains optional. Build a new release rather than replacing existing wheel contents or image tags.

`scripts/vendor_reader.py` builds from an explicit committed revision at this repository root. It emits a wheel, native `requirements.lock` and manifest carrying the public source URL, full revision and hashes. `.github/workflows/package.yml` tests native platforms and the SDK, then creates immutable GitHub package releases from matching version tags. `.github/workflows/publish-images.yml` is reusable image publication called from the backend with a pinned source SHA and caller AWS identity; no new cloud permissions or automatic customer-image pushes belong in package setup.

`CLAUDE.md` points here. Update this map when adding, moving or deleting files. Write documentation paragraphs on one source line. Keep source comments about durable constraints rather than investigation history.

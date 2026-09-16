# Customer metrics-build example

`job.py` reads complete MCAP recording folders and emits inventory or explicitly selected Burro observations through `config.resim.yml`. This is editable analysis example code, not a robot software repository. The default inventory profile makes no robot-domain claims. The Burro profile requires valid recorded `OBSTACLE_STOP`/`STATE_OBSTACLE_STOP` constants and reports GPS text changes with independent state per recording; never invent thresholds or substitute a numeric mask.

The Dockerfile copies only job/config into the runtime. Its sibling `test` and `test-results` stages execute maintained synthetic MCAP-to-SDK checks and export a receipt. The exact-file `.dockerignore` allowlist excludes other context, recordings and receipts. README.md supplies standalone Finch instructions; follow `tests/AGENTS.md` for the checks. Changes require fresh tests and source hashes; historical published images/configs are separate immutable artifacts.

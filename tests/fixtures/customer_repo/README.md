# Customer metrics-build example

This editable example reads a complete folder of MCAP recordings, including nested files, and writes validated SDK emissions. It contains no robot application or fictional robot implementation. The default `inventory` profile records per-file/topic message counts, exact session bounds and a Recording started inventory marker; that marker is not a fault detector.

The explicit `burro` profile reports the recorded obstacle-stop bit and GPS fix-status changes. It accepts the embedded `OBSTACLE_STOP` or `STATE_OBSTACLE_STOP` constant, requires positive single-bit integer definitions to agree, and applies it to `state`, not `enabled_state`. GPS `fix_status` is recorded text. State resets for each file; an initially active stop is an observation, not a claimed onset. No guessed failure threshold or sparse-message duration/rate is inferred. Missing or invalid definitions fail instead of supplying a numeric fallback.

The `session_inventory` topic preserves bounds as decimal nanosecond strings; duration SQL casts to BIGINT before subtraction, then divides by DOUBLE seconds. The `Session metrics` set contains inventory metrics; `Burro observations` adds source-dependent stop/GPS counts. Missing sources do not become zero observations.

`job.py`, `config.resim.yml` and the digest-pinned Dockerfile form the runtime. The maintained `tests/` scripts generate synthetic MCAPs and validate the real SDK entrypoint independently of the enclosing repository. Customer-specific signal selection, code semantics and thresholds belong in your edited job/config and corresponding tests.

## Build and test from this directory

This directory is a self-contained synthetic build context. Its maintained `tests/make_fixture.py`, `tests/check_emissions.py` and `tests/run_checks.py` generate synthetic recordings, execute the actual job with the real SDK, validate emissions and export a receipt/log. They require only the digest-pinned Linux base; no enclosing ReSim checkout, native SDK installation, downloaded recording or remembered chat is needed. Keep job/config edits and their tests together here. The `test` stage selects the Burro fixture explicitly; the `runtime` target retains the job's existing inventory default, with Burro selected explicitly at execution through `SESSION_ROBOT_PROFILE` or `--robot-profile`.

Using the connected AWS Finch MCP's `finch_build_container_image`, replace the placeholders with absolute paths visible to Finch. Set `context_path` to this directory and export to a fresh, empty, customer-owned host directory outside this build context (on macOS, use an already shared workspace path):

```json
{
  "dockerfile_path": "<absolute-customer-source>/Dockerfile",
  "context_path": "<absolute-customer-source>",
  "target": "test-results",
  "platforms": ["linux/amd64"],
  "no_cache": true,
  "quiet": false,
  "progress": "plain",
  "outputs": "type=local,dest=<fresh-host-results-directory>"
}
```

Require tool success and inspect `receipt.json` plus `log.txt`. The receipt records the executed commands, Linux architecture, Python version, source/config/test hashes and log hash. Verify those hashes against the files you just tested. The log must include SDK import, one `FIXTURE PASS`, two `EMISSIONS PASS`, two `FAILURE PASS` and one `SDK CHECK PASS`. The checks assert 35 rows across five topics, 20 messages, ten events, both state-mask aliases, independent recording state, exact timestamps/nested keys, schema-valid emissions and cleanup on invalid input. A failed command or missing marker leaves no successful receipt. Generic build success without readable receipts is insufficient. These synthetic expectations cover the Burro fixture; changing domain logic or adding another profile requires corresponding test cases, not just editing expected totals.

The explicit test-stage `RUN` executes `python /checks/run_checks.py`, which imports the real SDK, runs `python /checks/make_fixture.py /tmp/resim/inputs/experience`, executes `python /app/job.py`, then runs `python /checks/check_emissions.py /app/config.resim.yml /tmp/resim/outputs/emissions.resim.jsonl`. An `ENTRYPOINT` declaration alone does not run tests during an image build. No AWS publication is needed to run this synthetic check.

After those checks pass, build the runtime from the same unchanged source and pinned base:

```json
{
  "dockerfile_path": "<absolute-customer-source>/Dockerfile",
  "context_path": "<absolute-customer-source>",
  "target": "runtime",
  "platforms": ["linux/amd64"],
  "tags": ["<confirmed-registry>/<repository>:<new-version>"],
  "no_cache": true,
  "quiet": false,
  "progress": "plain"
}
```

The runtime target excludes tests, generated data and receipts; it does not rerun the sibling test stage. Any job, config, dependency or test change requires a fresh test build. Keep source/test hashes, commands and results outside this build context. Before any AWS write or push, verify the effective AWS identity used by Finch and obtain explicit confirmation of caller account/role, region and destination registry/repository. Then publish through the connected MCP, verify the registry digest and record the registered build ID/version. Evaluation requires its separate client approval. This README does not authorize publication or evaluation by itself.

When adding an analysis module, dependency input or test, update both the explicit Dockerfile `COPY` instructions and the exact-file `.dockerignore` allowlist, rerun the tests and capture the updated context manifest. Keep directory-wide exceptions out of the allowlist. The SDK receipt hashes its six source/config/test inputs; it is not a complete build-context manifest. Record every included context file separately, including `.dockerignore` and Dockerfile, with its SHA-256 outside this build context.

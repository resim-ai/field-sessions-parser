"""Persist evidence from executed checks; a failed run cannot leave a success receipt."""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from pathlib import Path


def run_checks(commands, results_dir, *, markers, metadata):
    results_dir.mkdir(parents=True, exist_ok=True)
    receipt = results_dir / "receipt.json"
    pending = results_dir / "receipt.json.pending"
    receipt.unlink(missing_ok=True)
    pending.unlink(missing_ok=True)
    log_path = results_dir / "log.txt"
    with log_path.open("w") as log:
        for command in commands:
            log.write("COMMAND " + json.dumps(command) + "\n")
            log.flush()
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False)
            log.write(f"EXIT {result.returncode}\n")
            log.flush()
            if result.returncode:
                print(log_path.read_text(), file=sys.stderr)
                raise subprocess.CalledProcessError(result.returncode, command)
    contents = log_path.read_text()
    for marker, count in markers.items():
        if sum(line.startswith(marker) for line in contents.splitlines()) != count:
            raise RuntimeError(f"Missing or unexpected check output: {marker}")
    result = {
        "status": "passed",
        "commands": commands,
        "expected_markers": markers,
        "log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
        **metadata,
    }
    pending.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    pending.replace(receipt)
    print(contents, end="")
    print("SDK RECEIPT PASS: /test-results/receipt.json", flush=True)


def main():
    python = sys.executable
    files = [
        Path("/app/job.py"),
        Path("/app/config.resim.yml"),
        *sorted(Path("/checks").glob("*.py")),
        Path("/checks/Dockerfile"),
    ]
    run_checks(
        [
            [
                python,
                "-c",
                "from resim.sdk.metrics.emissions import Emitter; "
                "from importlib.metadata import version; "
                "print('SDK IMPORT PASS:', version('resim-open-core'))",
            ],
            [python, "/checks/make_fixture.py", "/tmp/resim/inputs/experience"],
            [python, "/app/job.py"],
            [
                python,
                "/checks/check_emissions.py",
                "/app/config.resim.yml",
                "/tmp/resim/outputs/emissions.resim.jsonl",
            ],
        ],
        Path("/test-results"),
        markers={
            "SDK IMPORT PASS:": 1,
            "FIXTURE PASS:": 1,
            "EMISSIONS PASS:": 2,
            "FAILURE PASS:": 2,
            "SDK CHECK PASS:": 1,
        },
        metadata={
            "python_version": platform.python_version(),
            "machine": platform.machine(),
            "system": platform.system(),
            "source_sha256": {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files},
        },
    )


if __name__ == "__main__":
    main()

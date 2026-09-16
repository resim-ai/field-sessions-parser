"""Build a portable reader bundle from an immutable repository revision."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
from pathlib import Path


def git(repo: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(repo), *args])


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True, help="Committed source revision; never the working tree")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--python", default="3.12", help="Build interpreter (runtime requirement remains 3.12)"
    )
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    revision = git(repo, "rev-parse", "--verify", f"{args.revision}^{{commit}}").decode().strip()
    epoch = git(repo, "show", "-s", "--format=%ct", revision).decode().strip()
    paths = ["pyproject.toml", "README.md", "src", "requirements.lock", "requirements-readers.lock"]
    archive = git(repo, "archive", "--format=tar", revision, *paths)

    with tempfile.TemporaryDirectory(prefix="session-reader-bundle-") as temporary:
        scratch = Path(temporary)
        with tarfile.open(fileobj=io.BytesIO(archive)) as source:
            source.extractall(scratch, filter="data")
        package = scratch
        project = tomllib.loads((package / "pyproject.toml").read_text())["project"]
        wheel_name = f"field_sessions_parser-{project['version']}-py3-none-any.whl"
        wheels = scratch / "wheels"
        subprocess.run(
            [
                "uv",
                "build",
                "--wheel",
                "--python",
                args.python,
                "--no-python-downloads",
                "--build-constraints",
                "requirements.lock",
                "--require-hashes",
                "--out-dir",
                str(wheels),
            ],
            cwd=package,
            env={**os.environ, "SOURCE_DATE_EPOCH": epoch},
            check=True,
        )
        wheel = wheels / wheel_name
        lock = package / "requirements-readers.lock"
        manifest = {
            "wheel": wheel_name,
            "sha256": sha256(wheel),
            "requirements_sha256": sha256(lock),
            "source_revision": revision,
            "source_repository": "https://github.com/resim-ai/field-sessions-parser",
            "python_version": "3.12",
        }
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".reader-bundle-", dir=output) as staging:
            staged = Path(staging)
            shutil.copyfile(wheel, staged / wheel_name)
            shutil.copyfile(lock, staged / "requirements.lock")
            (staged / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
            for name in [wheel_name, "requirements.lock", "manifest.json"]:
                (staged / name).replace(output / name)
        print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()

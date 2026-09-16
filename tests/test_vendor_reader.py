"""Bundle provenance must come from committed standalone source, not local edits."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], text=True).strip()


@pytest.fixture
def source(tmp_path):
    repo = tmp_path / "package checkout"
    repo.mkdir()
    (repo / "scripts").mkdir()
    shutil.copyfile(ROOT / "scripts/vendor_reader.py", repo / "scripts/vendor_reader.py")
    (repo / "src").mkdir()
    (repo / "src/committed.py").write_text("committed source\n")
    (repo / "README.md").write_text("committed README\n")
    (repo / "pyproject.toml").write_text('[project]\nname = "field-sessions-parser"\nversion = "0.2.1"\n')
    (repo / "requirements.lock").write_text("build constraints\n")
    (repo / "requirements-readers.lock").write_text("native constraints\n")
    git(repo, "init", "--initial-branch=main")
    git(repo, "add", ".")
    git(repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "Source")
    revision = git(repo, "rev-parse", "HEAD")
    (repo / "src/committed.py").write_text("uncommitted source\n")
    (repo / "src/untracked.py").write_text("untracked source\n")
    (repo / "pyproject.toml").write_text('[project]\nversion = "99.0.0"\n')
    (repo / "README.md").write_text("uncommitted README\n")
    return repo, revision


def builder(tmp_path, body):
    binaries = tmp_path / "bin"
    binaries.mkdir()
    uv = binaries / "uv"
    uv.write_text(f"#!{sys.executable}\n" + body)
    uv.chmod(0o700)
    return {**os.environ, "PATH": str(binaries) + os.pathsep + os.environ.get("PATH", "")}


def invoke(source, output, env, revision=None):
    repo, committed = source
    return subprocess.run(
        [
            sys.executable,
            str(repo / "scripts/vendor_reader.py"),
            "--revision",
            revision or committed,
            "--output",
            str(output),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_standalone_archive_excludes_local_edits_and_records_public_source(source, tmp_path):
    env = builder(
        tmp_path,
        """import json, os, sys
from pathlib import Path
assert '--require-hashes' in sys.argv
assert '--no-python-downloads' in sys.argv
assert sys.argv[sys.argv.index('--build-constraints') + 1] == 'requirements.lock'
assert not Path('src/untracked.py').exists()
assert Path('src/committed.py').read_text() == 'committed source\\n'
assert Path('README.md').read_text() == 'committed README\\n'
assert '0.2.1' in Path('pyproject.toml').read_text()
out = Path(sys.argv[sys.argv.index('--out-dir') + 1])
out.mkdir()
(out / 'field_sessions_parser-0.2.1-py3-none-any.whl').write_text(
    json.dumps({'epoch': os.environ['SOURCE_DATE_EPOCH']}, sort_keys=True))
""",
    )
    output = tmp_path / "bundle"
    result = invoke(source, output, env)
    assert result.returncode == 0, result.stderr
    manifest = json.loads((output / "manifest.json").read_text())
    assert manifest == json.loads(result.stdout)
    assert manifest["source_repository"] == "https://github.com/resim-ai/field-sessions-parser"
    assert manifest["source_revision"] == source[1]
    assert manifest["python_version"] == "3.12"
    assert manifest["wheel"] == "field_sessions_parser-0.2.1-py3-none-any.whl"
    wheel = output / manifest["wheel"]
    assert manifest["sha256"] == hashlib.sha256(wheel.read_bytes()).hexdigest()
    lock = output / "requirements.lock"
    assert lock.read_text() == "native constraints\n"
    assert manifest["requirements_sha256"] == hashlib.sha256(lock.read_bytes()).hexdigest()
    assert json.loads(wheel.read_text())["epoch"] == git(source[0], "show", "-s", "--format=%ct", source[1])
    again = invoke(source, tmp_path / "repeat", env)
    assert again.returncode == 0, again.stderr
    assert json.loads(again.stdout) == manifest


def test_build_failure_keeps_existing_bundle_untouched(source, tmp_path):
    env = builder(tmp_path, "import sys\nsys.exit(17)\n")
    output = tmp_path / "bundle"
    output.mkdir()
    (output / "manifest.json").write_text("previous manifest\n")
    (output / "requirements.lock").write_text("previous lock\n")
    result = invoke(source, output, env)
    assert result.returncode != 0
    assert "17" in result.stderr
    assert (output / "manifest.json").read_text() == "previous manifest\n"
    assert (output / "requirements.lock").read_text() == "previous lock\n"
    assert not list(output.glob("*.whl"))


def test_missing_revision_cannot_create_bundle(source, tmp_path):
    output = tmp_path / "bundle"
    result = invoke(source, output, os.environ.copy(), revision="missing-commit")
    assert result.returncode != 0
    assert not output.exists()

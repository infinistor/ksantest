"""Generate HTML report from pytest JUnit XML output."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pytest and generate HTML report")
    parser.add_argument("--config", default="config.ini", help="INI config path")
    parser.add_argument("--results-dir", default="results", help="JUnit XML output directory")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    repo_root = root.parent
    results_dir = root / args.results_dir
    results_dir.mkdir(exist_ok=True)
    junit_xml = results_dir / "junit.xml"

    env = {**dict(**__import__("os").environ), "S3TESTS_INI": str(root / args.config)}

    subprocess.run(
        [sys.executable, "-m", "pytest", "-v", f"--junitxml={junit_xml}", "s3tests/tests"],
        cwd=root,
        env=env,
        check=True,
    )

    if not junit_xml.is_file():
        print(f"Error: expected JUnit XML at {junit_xml}", file=sys.stderr)
        return 1

    xunit_dir = repo_root / "xunit-to-html"
    result_xml = xunit_dir / "Result_python.xml"
    shutil.copyfile(junit_xml, result_xml)

    subprocess.run(
        [
            "java",
            "-jar",
            str(xunit_dir / "saxon9he.jar"),
            f"-o:{xunit_dir / 'Result_python.html'}",
            f"-s:{result_xml}",
            f"-xsl:{xunit_dir / 'xunit_to_html.xsl'}",
        ],
        check=True,
    )
    print(f"Report: {xunit_dir / 'Result_python.html'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

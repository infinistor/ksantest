"""Resolve Java-style (class, method) args to a pytest node id."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_CLASS_RE = re.compile(r"^class\s+(Test\w+)\s*\(", re.MULTILINE)
_METHOD_RE = re.compile(r"^\s+def\s+(test_\w+)\s*\(", re.MULTILINE)


def snake(name: str) -> str:
    name = name.strip()
    if re.fullmatch(r"[A-Z0-9]+", name):
        return name.lower()
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    return name.lower()


def pascal(name: str) -> str:
    if re.fullmatch(r"[A-Z0-9]+", name):
        return name
    return "".join(part[:1].upper() + part[1:].lower() for part in snake(name).split("_") if part)


def _discover_class(source: str, preferred: str | None = None) -> str:
    classes = _CLASS_RE.findall(source)
    if not classes:
        raise FileNotFoundError("No Test* class found in module")
    if preferred and preferred in classes:
        return preferred
    if preferred:
        preferred_lower = preferred.lower()
        for name in classes:
            if name.lower() == preferred_lower:
                return name
    if len(classes) == 1:
        return classes[0]
    raise FileNotFoundError(f"Ambiguous Test* classes in module: {', '.join(classes)}")


def _ensure_method(source: str, method: str) -> None:
    methods = set(_METHOD_RE.findall(source))
    if method not in methods:
        raise FileNotFoundError(f"Method '{method}' not found in module")


def resolve(class_arg: str, method_arg: str, tests_root: Path) -> str:
    class_input = class_arg.strip()
    method = method_arg.strip()
    if "_" not in method:
        method = snake(method)

    stem = class_input
    if stem.startswith("Test") and len(stem) > 4 and stem[4].isupper():
        stem = stem[4:]

    if stem.startswith("test_"):
        file_stem = stem
    else:
        file_stem = f"test_{snake(stem)}"

    if class_input.startswith("Test") and len(class_input) > 4 and class_input[4].isupper():
        preferred_class = class_input
    elif class_input.startswith("test_"):
        preferred_class = "Test" + pascal(class_input[5:])
    else:
        preferred_class = "Test" + pascal(stem)

    rel = Path("s3tests") / "tests" / f"{file_stem}.py"
    abs_path = tests_root / rel
    if not abs_path.is_file():
        raise FileNotFoundError(f"Test file not found for class '{class_arg}': {abs_path}")

    source = abs_path.read_text(encoding="utf-8")
    pytest_class = _discover_class(source, preferred_class)
    _ensure_method(source, method)

    return f"{rel.as_posix()}::{pytest_class}::{method}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("test_class")
    parser.add_argument("test_method")
    parser.add_argument(
        "--root",
        default=str(Path(__file__).resolve().parents[1]),
        help="python/ project root",
    )
    args = parser.parse_args()
    try:
        print(resolve(args.test_class, args.test_method, Path(args.root)))
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

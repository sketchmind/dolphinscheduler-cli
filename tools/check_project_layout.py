from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "dsctl"

FORBIDDEN_PATHS = [
    SRC / "authoring",
    SRC / "cli",
    SRC / "domains",
    SRC / "integration",
    SRC / "search",
    SRC / "types",
    SRC / "workflow_spec",
]

REQUIRED_PATHS = [
    SRC / "generated",
    SRC / "generated" / "versions",
    SRC / "upstream",
    SRC / "client.py",
    SRC / "config.py",
    SRC / "errors.py",
    SRC / "output.py",
]


def main() -> None:
    forbidden = [path for path in FORBIDDEN_PATHS if path.exists()]
    if forbidden:
        formatted = ", ".join(str(path.relative_to(ROOT)) for path in forbidden)
        message = f"project layout violation: forbidden paths present: {formatted}"
        raise SystemExit(message)

    missing = [path for path in REQUIRED_PATHS if not path.exists()]
    if missing:
        formatted = ", ".join(str(path.relative_to(ROOT)) for path in missing)
        message = f"project layout violation: required paths missing: {formatted}"
        raise SystemExit(message)


if __name__ == "__main__":
    main()

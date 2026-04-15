from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "src" / "dsctl"
GENERATED_ROOT = SRC_ROOT / "generated"
ALLOWLIST_PATH = ROOT / "tools" / "explicit_object_allowlist.txt"
DEBT_PATH = ROOT / "tools" / "explicit_object_debt.txt"


@dataclass(frozen=True, order=True)
class Finding:
    path: str
    context: str
    snippet: str

    def baseline_key(self) -> str:
        return f"{self.path}|{self.context}|{self.snippet}"


class ExplicitObjectVisitor(ast.NodeVisitor):
    def __init__(self, *, relative_path: str, source: str) -> None:
        self.relative_path = relative_path
        self.source = source
        self.scope: list[str] = []
        self.findings: set[Finding] = set()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        target_text = ast.unparse(node.target).strip()
        if self._is_type_alias(node.annotation) and node.value is not None:
            self._record_if_explicit_object(
                node.value,
                context=f"{self._qualname()}.{target_text}:type-alias"
                if self.scope
                else f"{target_text}:type-alias",
            )
        else:
            self._record_if_explicit_object(
                node.annotation,
                context=f"{self._qualname()}.{target_text}"
                if self.scope
                else target_text,
            )
        self.generic_visit(node)

    def _visit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        self.scope.append(node.name)
        for arg in (
            [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
            + ([node.args.vararg] if node.args.vararg is not None else [])
            + ([node.args.kwarg] if node.args.kwarg is not None else [])
        ):
            if arg is None or arg.annotation is None:
                continue
            self._record_if_explicit_object(
                arg.annotation,
                context=f"{self._qualname()}({arg.arg})",
            )
        if node.returns is not None:
            self._record_if_explicit_object(
                node.returns,
                context=f"{self._qualname()}:return",
            )
        self.generic_visit(node)
        self.scope.pop()

    def _qualname(self) -> str:
        return ".".join(self.scope)

    def _record_if_explicit_object(self, node: ast.AST, *, context: str) -> None:
        if not _contains_explicit_object(node):
            return
        snippet = (
            ast.get_source_segment(self.source, node)
            or ast.unparse(node)
            or "<unknown>"
        )
        self.findings.add(
            Finding(
                path=self.relative_path,
                context=context,
                snippet=" ".join(snippet.split()),
            )
        )

    @staticmethod
    def _is_type_alias(node: ast.expr) -> bool:
        return isinstance(node, ast.Name) and node.id == "TypeAlias"


def _contains_explicit_object(node: ast.AST | None) -> bool:
    if node is None:
        return False
    if isinstance(node, ast.Name) and node.id == "object":
        return True
    return any(_contains_explicit_object(child) for child in ast.iter_child_nodes(node))


def collect_findings(root: Path) -> list[Finding]:
    findings: set[Finding] = set()
    for path in sorted(root.rglob("*.py")):
        if path.is_relative_to(GENERATED_ROOT):
            continue
        relative_path = path.relative_to(ROOT).as_posix()
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        visitor = ExplicitObjectVisitor(relative_path=relative_path, source=source)
        visitor.visit(tree)
        findings.update(visitor.findings)
    return sorted(findings)


def load_entries(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }


def write_entries(path: Path, findings: list[Finding], *, header: str) -> None:
    lines = [
        header,
        "# Each line is path|context|snippet.",
        *[finding.baseline_key() for finding in findings],
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--write-allowlist",
        action="store_true",
        help="rewrite the explicit-object allowlist to match current findings",
    )
    mode_group.add_argument(
        "--write-debt",
        action="store_true",
        help="rewrite the explicit-object debt file to match current findings",
    )
    args = parser.parse_args()

    findings = collect_findings(SRC_ROOT)
    if args.write_allowlist:
        write_entries(
            ALLOWLIST_PATH,
            findings,
            header="# Reviewed allowlist for intentional explicit `object` usage.",
        )
        print(f"wrote {len(findings)} explicit-object findings to {ALLOWLIST_PATH}")
        return 0
    if args.write_debt:
        write_entries(
            DEBT_PATH,
            findings,
            header="# Reviewed debt for explicit `object` usage pending cleanup.",
        )
        print(f"wrote {len(findings)} explicit-object findings to {DEBT_PATH}")
        return 0

    allowlist = load_entries(ALLOWLIST_PATH)
    debt = load_entries(DEBT_PATH)
    duplicate_entries = sorted(allowlist & debt)
    current = {finding.baseline_key() for finding in findings}
    reviewed = allowlist | debt
    unexpected = sorted(current - reviewed)
    stale_allowlist = sorted(allowlist - current)
    stale_debt = sorted(debt - current)

    if (
        not unexpected
        and not stale_allowlist
        and not stale_debt
        and not duplicate_entries
    ):
        print(
            "explicit-object audit passed with "
            f"{len(allowlist)} allowlisted and {len(debt)} debt findings"
        )
        return 0

    if duplicate_entries:
        print("duplicate explicit-object entries found in both allowlist and debt:")
        for item in duplicate_entries:
            print(f"  ! {item}")
    if unexpected:
        print("unexpected explicit-object findings:")
        for item in unexpected:
            print(f"  + {item}")
    if stale_allowlist:
        print("stale explicit-object allowlist entries:")
        for item in stale_allowlist:
            print(f"  - {item}")
    if stale_debt:
        print("stale explicit-object debt entries:")
        for item in stale_debt:
            print(f"  - {item}")
    print(
        "update the code or refresh the reviewed files with "
        "`python tools/check_explicit_object.py --write-allowlist` or "
        "`python tools/check_explicit_object.py --write-debt`"
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

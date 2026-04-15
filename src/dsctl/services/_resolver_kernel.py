from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from dsctl.errors import ApiResultError, NotFoundError, ResolutionError, UserInputError
from dsctl.services.pagination import PageRecord, PageResult, collect_all_pages

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Mapping, Sequence

ResolvedT = TypeVar("ResolvedT")
RecordT = TypeVar("RecordT")
PayloadT = TypeVar("PayloadT")


def normalize_identifier(identifier: str, *, label: str) -> str:
    """Return one trimmed identifier or raise one user-input error."""
    normalized_identifier = identifier.strip()
    if normalized_identifier:
        return normalized_identifier
    message = f"{label} identifier must not be empty"
    raise UserInputError(
        message,
        suggestion=f"Pass one non-empty {label} name or numeric id.",
    )


def parse_code(identifier: str) -> int | None:
    """Parse one decimal identifier into an integer when possible."""
    try:
        return int(identifier)
    except ValueError:
        return None


def resolve_direct(
    key: int,
    *,
    load: Callable[[int], PayloadT],
    project: Callable[[PayloadT], ResolvedT],
    not_found_message: str,
    not_found_details: Mapping[str, object],
) -> ResolvedT:
    """Resolve one resource by direct id/code lookup."""
    try:
        payload = load(key)
    except ApiResultError as exc:
        raise NotFoundError(not_found_message, details=not_found_details) from exc
    return project(payload)


def require_single_match(
    matches: Sequence[ResolvedT],
    *,
    not_found_message: str,
    not_found_details: Mapping[str, object],
    ambiguous_message: str,
    ambiguous_details: Mapping[str, object],
) -> ResolvedT:
    """Require exactly one resolved match or raise one structured error."""
    if not matches:
        raise NotFoundError(not_found_message, details=not_found_details)
    if len(matches) > 1:
        raise ResolutionError(ambiguous_message, details=ambiguous_details)
    return matches[0]


def collect_resolution_page_items(
    *,
    fetch_page: Callable[[int, int], PageRecord[RecordT]],
    page_size: int,
    max_pages: int,
    safety_message: str,
    safety_details: Mapping[str, object],
) -> Sequence[RecordT]:
    """Collect one bounded page stream for resolver exact-match filtering."""
    try:
        searched: PageResult[RecordT] = collect_all_pages(
            fetch_page,
            page_no=1,
            page_size=page_size,
            max_pages=max_pages,
        )
    except UserInputError as exc:
        raise ResolutionError(
            safety_message,
            details={**dict(safety_details), **exc.details},
        ) from exc
    return searched.items


def resolve_exact_matches(
    records: Sequence[RecordT],
    *,
    matches: Callable[[RecordT], bool],
    project: Callable[[RecordT], ResolvedT],
    dedupe_key: Callable[[ResolvedT], Hashable] | None = None,
) -> list[ResolvedT]:
    """Project exact matches and optionally de-duplicate repeated rows."""
    resolved_matches = [project(record) for record in records if matches(record)]
    if dedupe_key is None:
        return resolved_matches

    unique_matches: list[ResolvedT] = []
    seen_keys: set[Hashable] = set()
    for resolved in resolved_matches:
        key = dedupe_key(resolved)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_matches.append(resolved)
    return unique_matches

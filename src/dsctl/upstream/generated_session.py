from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, TypeGuard, cast
from urllib.parse import urlsplit

from dsctl.errors import ApiTransportError

if TYPE_CHECKING:
    from typing import Unpack

    from dsctl.client import (
        DolphinSchedulerClient,
        HttpFormValue,
        HttpQueryParams,
        HttpQueryValue,
        HttpRequestData,
        MultipartFiles,
    )
    from dsctl.generated.versions.ds_3_4_1.api.operations._base import RequestKwargs
    from dsctl.support.json_types import JsonValue


class GeneratedSessionAdapter:
    """Adapt the shared `DolphinSchedulerClient` to the generated session protocol."""

    def __init__(self, client: DolphinSchedulerClient, *, base_url: str) -> None:
        """Create a generated-session bridge over one shared HTTP client."""
        self._client = client
        self._base_url = base_url.rstrip("/")

    def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        **kwargs: Unpack[RequestKwargs],
    ) -> JsonValue:
        """Route a generated operation call through the shared HTTP client."""
        try:
            payload = self._client.request_payload(
                method,
                _relative_path(url, base_url=self._base_url),
                params=_query_params_or_none(kwargs.pop("params", None)),
                json_body=_json_value_or_none(kwargs.pop("json", None)),
                form_data=_request_data_or_none(kwargs.pop("data", None)),
                content=kwargs.pop("content", None),
                files=_multipart_files_or_none(kwargs.pop("files", None)),
                headers=headers,
            )
            _reject_unexpected_request_kwargs(kwargs)
        except TypeError as exc:
            message = f"Generated request shape did not match adapter contract: {exc}"
            raise ApiTransportError(
                message,
                details={
                    "method": method.upper(),
                    "url": url,
                },
            ) from exc
        return payload


def _relative_path(url: str, *, base_url: str) -> str:
    if url == base_url:
        return ""
    prefix = f"{base_url}/"
    if url.startswith(prefix):
        return url.removeprefix(prefix)
    return urlsplit(url).path.lstrip("/")


def _query_params_or_none(value: object) -> HttpQueryParams | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        message = f"Expected query param mapping, got {type(value)!r}"
        raise TypeError(message)
    cleaned: dict[str, HttpQueryValue] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            message = f"Expected string query param key, got {type(key)!r}"
            raise TypeError(message)
        if _is_http_query_scalar(item):
            cleaned[key] = item
            continue
        if isinstance(item, Sequence) and not isinstance(
            item,
            (str, bytes, bytearray),
        ):
            sequence = list(item)
            if all(_is_http_query_scalar(entry) for entry in sequence):
                cleaned[key] = sequence
                continue
        message = f"Unsupported query param value type for {key!r}: {type(item)!r}"
        raise TypeError(message)
    return cleaned


def _request_data_or_none(
    value: object,
) -> HttpRequestData | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        cleaned: dict[str, HttpFormValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                message = f"Expected string form field key, got {type(key)!r}"
                raise TypeError(message)
            if _is_http_form_scalar(item):
                cleaned[key] = item
                continue
            if isinstance(item, Sequence) and not isinstance(
                item,
                (str, bytes, bytearray),
            ):
                sequence = list(item)
                if all(_is_http_form_scalar(entry) for entry in sequence):
                    cleaned[key] = sequence
                    continue
            message = f"Unsupported form field value type for {key!r}: {type(item)!r}"
            raise TypeError(message)
        return cleaned
    if isinstance(value, str | bytes | bytearray):
        return value
    message = f"Expected request data payload, got {type(value)!r}"
    raise TypeError(message)


def _multipart_files_or_none(value: object) -> MultipartFiles | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return cast("MultipartFiles", value)
    message = f"Expected multipart file mapping, got {type(value)!r}"
    raise TypeError(message)


def _reject_unexpected_request_kwargs(kwargs: RequestKwargs) -> None:
    if not kwargs:
        return
    keys = ", ".join(sorted(kwargs))
    message = f"Unsupported generated request arguments: {keys}"
    raise TypeError(message)


def _json_value_or_none(value: object) -> JsonValue | None:
    if value is None:
        return None
    if _is_json_value(value):
        return value
    message = f"Expected JSON payload, got {type(value)!r}"
    raise TypeError(message)


def _is_json_value(value: object) -> TypeGuard[JsonValue]:
    if value is None or isinstance(value, str | int | float | bool):
        return True
    if isinstance(value, Mapping):
        return all(
            isinstance(key, str) and _is_json_value(item) for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return all(_is_json_value(item) for item in value)
    return False


def _is_http_query_scalar(value: object) -> bool:
    return value is None or isinstance(value, str | int | float | bool)


def _is_http_form_scalar(value: object) -> bool:
    return value is None or isinstance(value, str | bytes | int | float | bool)


__all__ = ["GeneratedSessionAdapter"]

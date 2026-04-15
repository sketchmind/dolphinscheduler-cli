from __future__ import annotations

import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, Self, TypeAlias, TypedDict, assert_never, cast

import httpx

from dsctl.auth import build_auth_headers
from dsctl.errors import ApiHttpError, ApiResultError, ApiTransportError
from dsctl.support.json_types import is_json_value

if TYPE_CHECKING:
    from types import TracebackType

    from dsctl.config import ClusterProfile
    from dsctl.support.json_types import JsonObject, JsonValue

HttpQueryScalar: TypeAlias = str | int | float | bool | None
HttpQueryValue: TypeAlias = HttpQueryScalar | Sequence[HttpQueryScalar]
HttpQueryParams: TypeAlias = Mapping[str, HttpQueryValue]
HttpFormScalar: TypeAlias = str | bytes | int | float | bool | None
HttpFormValue: TypeAlias = HttpFormScalar | Sequence[HttpFormScalar]
HttpFormData: TypeAlias = Mapping[str, HttpFormValue]
HttpRequestData: TypeAlias = HttpFormData | str | bytes | bytearray
HttpHeaders: TypeAlias = Mapping[str, str]
MultipartFileContent: TypeAlias = IO[bytes] | bytes | str
MultipartFileValue: TypeAlias = (
    MultipartFileContent
    | tuple[str | None, MultipartFileContent]
    | tuple[str | None, MultipartFileContent, str | None]
    | tuple[str | None, MultipartFileContent, str | None, Mapping[str, str]]
)
MultipartFiles: TypeAlias = Mapping[str, MultipartFileValue]


class RequestContext(TypedDict):
    """Structured metadata describing a single outbound HTTP request."""

    request_id: str
    method: str
    path: str
    url: str
    attempts: int
    max_attempts: int
    retryable: bool


@dataclass(frozen=True)
class BinaryResponse:
    """Binary response payload plus selected HTTP metadata."""

    content: bytes
    headers: dict[str, str]
    content_type: str | None


RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}
REQUEST_ID_HEADER = "X-Request-ID"


class DolphinSchedulerClient:
    """HTTP transport for DS REST calls with retry and envelope handling."""

    def __init__(
        self,
        profile: ClusterProfile,
        timeout: float = 10.0,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        """Create a client bound to a cluster profile and optional mock transport."""
        self.profile = profile
        self.timeout = timeout
        self.retry_attempts = max(1, profile.api_retry_attempts)
        self.retry_backoff_ms = max(0, profile.api_retry_backoff_ms)
        self._base_url = profile.api_url.rstrip("/")
        self._client = httpx.Client(
            headers=build_auth_headers(profile),
            timeout=timeout,
            transport=transport,
        )

    def close(self) -> None:
        """Close the underlying HTTPX client."""
        self._client.close()

    def __enter__(self) -> Self:
        """Enter a context manager scope."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the client when leaving a context manager scope."""
        self.close()

    def healthcheck(self) -> JsonObject:
        """Call the DS actuator health endpoint and require a JSON object."""
        payload, _ = self._request_json("GET", "/actuator/health", retryable=True)
        if not isinstance(payload, dict):
            message = "Health endpoint returned an unexpected payload"
            raise ApiTransportError(
                message,
                details={"body": payload},
            )
        return payload

    def get_result(
        self,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        json_body: JsonValue | None = None,
        retryable: bool = True,
    ) -> JsonValue:
        """Issue a GET request and require a DS result envelope."""
        payload, request_details = self._request_json(
            "GET",
            path,
            params=params,
            json_body=json_body,
            retryable=retryable,
        )
        return _unwrap_result(
            payload,
            method="GET",
            path=path,
            request_details=request_details,
        )

    def get_payload(
        self,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        json_body: JsonValue | None = None,
        retryable: bool = True,
    ) -> JsonValue:
        """Issue a GET request and unwrap a DS result envelope when present."""
        payload, request_details = self._request_json(
            "GET",
            path,
            params=params,
            json_body=json_body,
            retryable=retryable,
        )
        return _unwrap_result_if_present(
            payload,
            method="GET",
            path=path,
            request_details=request_details,
        )

    def get_binary(
        self,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        retryable: bool = True,
    ) -> BinaryResponse:
        """Issue a GET request and return the raw binary response body."""
        response, _ = self._request("GET", path, params=params, retryable=retryable)
        return BinaryResponse(
            content=response.content,
            headers=dict(response.headers),
            content_type=response.headers.get("content-type"),
        )

    def request_payload(
        self,
        method: str,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        json_body: JsonValue | None = None,
        form_data: HttpRequestData | None = None,
        content: str | bytes | bytearray | None = None,
        files: MultipartFiles | None = None,
        headers: HttpHeaders | None = None,
        retryable: bool | None = None,
    ) -> JsonValue:
        """Issue an arbitrary request and unwrap a DS result envelope if present."""
        normalized_retryable = (
            method.upper() == "GET" if retryable is None else retryable
        )
        payload, request_details = self._request_json(
            method,
            path,
            params=params,
            json_body=json_body,
            form_data=form_data,
            content=content,
            files=files,
            headers=headers,
            retryable=normalized_retryable,
        )
        return _unwrap_result_if_present(
            payload,
            method=method,
            path=path,
            request_details=request_details,
        )

    def request_result(
        self,
        method: str,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        json_body: JsonValue | None = None,
        form_data: HttpRequestData | None = None,
        content: str | bytes | bytearray | None = None,
        files: MultipartFiles | None = None,
        headers: HttpHeaders | None = None,
        retryable: bool | None = None,
    ) -> JsonValue:
        """Issue an arbitrary request and require a DS result envelope."""
        normalized_retryable = (
            method.upper() == "GET" if retryable is None else retryable
        )
        payload, request_details = self._request_json(
            method,
            path,
            params=params,
            json_body=json_body,
            form_data=form_data,
            content=content,
            files=files,
            headers=headers,
            retryable=normalized_retryable,
        )
        return _unwrap_result(
            payload,
            method=method,
            path=path,
            request_details=request_details,
        )

    def post_result(
        self,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        json_body: JsonValue | None = None,
        form_data: HttpRequestData | None = None,
        files: MultipartFiles | None = None,
        retryable: bool = False,
    ) -> JsonValue:
        """Issue a POST request and require a DS result envelope."""
        payload, request_details = self._request_json(
            "POST",
            path,
            params=params,
            json_body=json_body,
            form_data=form_data,
            files=files,
            retryable=retryable,
        )
        return _unwrap_result(
            payload,
            method="POST",
            path=path,
            request_details=request_details,
        )

    def post_payload(
        self,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        json_body: JsonValue | None = None,
        form_data: HttpRequestData | None = None,
        files: MultipartFiles | None = None,
        retryable: bool = False,
    ) -> JsonValue:
        """Issue a POST request and unwrap a DS result envelope if present."""
        payload, request_details = self._request_json(
            "POST",
            path,
            params=params,
            json_body=json_body,
            form_data=form_data,
            files=files,
            retryable=retryable,
        )
        return _unwrap_result_if_present(
            payload,
            method="POST",
            path=path,
            request_details=request_details,
        )

    def put_result(
        self,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        json_body: JsonValue | None = None,
        form_data: HttpRequestData | None = None,
        files: MultipartFiles | None = None,
        retryable: bool = False,
    ) -> JsonValue:
        """Issue a PUT request and require a DS result envelope."""
        payload, request_details = self._request_json(
            "PUT",
            path,
            params=params,
            json_body=json_body,
            form_data=form_data,
            files=files,
            retryable=retryable,
        )
        return _unwrap_result(
            payload,
            method="PUT",
            path=path,
            request_details=request_details,
        )

    def delete_result(
        self,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        retryable: bool = False,
    ) -> JsonValue:
        """Issue a DELETE request and require a DS result envelope."""
        payload, request_details = self._request_json(
            "DELETE",
            path,
            params=params,
            retryable=retryable,
        )
        return _unwrap_result(
            payload,
            method="DELETE",
            path=path,
            request_details=request_details,
        )

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        json_body: JsonValue | None = None,
        form_data: HttpRequestData | None = None,
        content: str | bytes | bytearray | None = None,
        files: MultipartFiles | None = None,
        headers: HttpHeaders | None = None,
        retryable: bool,
    ) -> tuple[JsonValue, RequestContext]:
        response, request_details = self._request(
            method,
            path,
            params=params,
            json_body=json_body,
            form_data=form_data,
            content=content,
            files=files,
            headers=headers,
            retryable=retryable,
        )
        try:
            payload = _require_json_value(response.json(), label="response body")
        except (TypeError, ValueError) as exc:
            message = "Response body was not valid JSON"
            raise ApiTransportError(
                message,
                details={
                    **_request_context_details(request_details),
                    "status_code": response.status_code,
                    "body": _truncate_text(response.text),
                },
            ) from exc
        return payload, request_details

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: HttpQueryParams | None = None,
        json_body: JsonValue | None = None,
        form_data: HttpRequestData | None = None,
        content: str | bytes | bytearray | None = None,
        files: MultipartFiles | None = None,
        headers: HttpHeaders | None = None,
        retryable: bool,
    ) -> tuple[httpx.Response, RequestContext]:
        url = self._build_url(path)
        max_attempts = self.retry_attempts if retryable else 1
        attempt = 1
        request_id = _build_request_id()
        _validate_body_inputs(form_data=form_data, content=content)
        form_mapping, raw_content = _split_request_data(form_data)
        if raw_content is None:
            raw_content = _clean_content(content)

        while True:
            try:
                response = self._client.request(
                    method,
                    url,
                    params=_clean_query_params(params),
                    json=json_body,
                    data=form_mapping,
                    content=raw_content,
                    files=files,
                    headers=_request_headers(request_id=request_id, extra=headers),
                )
            except httpx.HTTPError as exc:
                if attempt < max_attempts and _is_retryable_transport_error(exc):
                    _sleep_before_retry(self.retry_backoff_ms, attempt)
                    attempt += 1
                    continue
                message = f"Request failed: {exc}"
                raise ApiTransportError(
                    message,
                    details=_request_context_details(
                        _request_context(
                            method=method,
                            path=path,
                            url=url,
                            attempts=attempt,
                            max_attempts=max_attempts,
                            retryable=retryable,
                            request_id=request_id,
                        )
                    ),
                ) from exc

            request_details = _request_context(
                method=method,
                path=path,
                url=url,
                attempts=attempt,
                max_attempts=max_attempts,
                retryable=retryable,
                request_id=request_id,
            )
            if response.status_code >= 400:
                if (
                    attempt < max_attempts
                    and response.status_code in RETRYABLE_STATUS_CODES
                ):
                    _sleep_before_retry(self.retry_backoff_ms, attempt)
                    attempt += 1
                    continue
                message = f"Request failed with HTTP {response.status_code}"
                raise ApiHttpError(
                    message,
                    status_code=response.status_code,
                    body=_decode_response_body(response),
                    details=_request_context_details(request_details),
                )
            return response, request_details

    def _build_url(self, path: str) -> str:
        return f"{self._base_url}/{path.lstrip('/')}"


def _clean_query_params(
    params: HttpQueryParams | None,
) -> dict[str, HttpQueryValue] | None:
    if params is None:
        return None
    cleaned: dict[str, HttpQueryValue] = {}
    for key, value in params.items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            cleaned[key] = value
            continue
        if isinstance(value, Sequence) and not isinstance(
            value, (str, bytes, bytearray)
        ):
            sequence = list(value)
            if all(
                isinstance(item, (str, int, float, bool)) or item is None
                for item in sequence
            ):
                cleaned[key] = sequence
                continue
        message = "Query params included an unsupported value type"
        raise ApiTransportError(
            message,
            details={"param": key, "value": repr(value)},
        )
    return cleaned


def _split_request_data(
    data: HttpRequestData | None,
) -> tuple[HttpFormData | None, str | bytes | None]:
    if data is None:
        return None, None
    if isinstance(data, Mapping):
        cleaned: dict[str, HttpFormValue] = {}
        for key, value in data.items():
            if value is None:
                continue
            if isinstance(value, Sequence) and not isinstance(
                value, (str, bytes, bytearray)
            ):
                items = [item for item in value if item is not None]
                if items:
                    cleaned[key] = items
                continue
            cleaned[key] = value
        return cleaned, None
    if isinstance(data, (str, bytes, bytearray)):
        return None, _clean_content(data)
    assert_never(data)


def _clean_content(value: str | bytes | bytearray | None) -> str | bytes | None:
    if value is None:
        return None
    if isinstance(value, bytearray):
        return bytes(value)
    return value


def _validate_body_inputs(
    *,
    form_data: HttpRequestData | None,
    content: str | bytes | bytearray | None,
) -> None:
    if form_data is None or content is None:
        return
    message = "Request body cannot include both form data and raw content"
    raise ApiTransportError(
        message,
        details={"form_data": repr(form_data), "content": repr(content)},
    )


def _request_headers(*, request_id: str, extra: HttpHeaders | None) -> dict[str, str]:
    headers = {REQUEST_ID_HEADER: request_id}
    if extra is None:
        return headers
    headers.update(
        {
            str(key): str(value)
            for key, value in extra.items()
            if isinstance(key, str) and isinstance(value, str)
        }
    )
    return headers


def _unwrap_result(
    payload: JsonValue,
    *,
    method: str,
    path: str,
    request_details: RequestContext | None = None,
) -> JsonValue:
    if not isinstance(payload, dict):
        message = "API response was not a JSON object"
        raise ApiTransportError(
            message,
            details={"body": payload},
        )

    result_code = payload.get("code")
    result_message = payload.get("msg")
    result_data = _result_payload_data(payload)
    if result_code != 0:
        details = dict(request_details or {})
        details.setdefault("method", method)
        details.setdefault("path", path)
        raise ApiResultError(
            result_code=result_code if isinstance(result_code, int) else None,
            result_message=str(
                result_message or "DolphinScheduler API returned an error"
            ),
            data=result_data,
            details=cast("dict[str, JsonValue]", details),
        )
    return result_data


def _unwrap_result_if_present(
    payload: JsonValue,
    *,
    method: str,
    path: str,
    request_details: RequestContext | None = None,
) -> JsonValue:
    if _looks_like_result_payload(payload):
        return _unwrap_result(
            payload,
            method=method,
            path=path,
            request_details=request_details,
        )
    return payload


def _looks_like_result_payload(payload: JsonValue) -> bool:
    return (
        isinstance(payload, dict)
        and {"code", "msg"}.issubset(payload.keys())
        and ("data" in payload or "dataList" in payload)
    )


def _result_payload_data(payload: dict[str, JsonValue]) -> JsonValue:
    if "data" in payload:
        return payload.get("data")
    return payload.get("dataList")


def _decode_response_body(response: httpx.Response) -> JsonValue:
    try:
        return _require_json_value(response.json(), label="response body")
    except (TypeError, ValueError):
        return _truncate_text(response.text)


def _require_json_value(value: object, *, label: str) -> JsonValue:
    if not is_json_value(value):
        message = f"{label} was not JSON-compatible"
        raise TypeError(message)
    return value


def _truncate_text(text: str, limit: int = 500) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}..."


def _request_context(
    *,
    method: str,
    path: str,
    url: str,
    attempts: int,
    max_attempts: int,
    retryable: bool,
    request_id: str,
) -> RequestContext:
    return {
        "request_id": request_id,
        "method": method.upper(),
        "path": path,
        "url": url,
        "attempts": attempts,
        "max_attempts": max_attempts,
        "retryable": retryable,
    }


def _request_context_details(request_details: RequestContext) -> dict[str, JsonValue]:
    return {
        "request_id": request_details["request_id"],
        "method": request_details["method"],
        "path": request_details["path"],
        "url": request_details["url"],
        "attempts": request_details["attempts"],
        "max_attempts": request_details["max_attempts"],
        "retryable": request_details["retryable"],
    }


def _is_retryable_transport_error(exc: httpx.HTTPError) -> bool:
    return isinstance(exc, httpx.RequestError)


def _sleep_before_retry(backoff_ms: int, attempt: int) -> None:
    if backoff_ms <= 0:
        return
    time.sleep((backoff_ms / 1000.0) * (2 ** (attempt - 1)))


def _build_request_id() -> str:
    return f"dsctl-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"

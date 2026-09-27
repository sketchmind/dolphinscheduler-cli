from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING

import httpx
import pytest

from dsctl.config import ConnectionSettings
from dsctl.errors import ConfigError
from dsctl.generated import version_discovery as discovery_facts
from dsctl.upstream import version_discovery as discovery
from dsctl.upstream.api_contract_discovery import ContractMatch

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dsctl.support.json_types import JsonValue

_PRODUCT_VERSIONS = ("3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3")
_OPENAPI_VERSIONS = ("3.2.0", "3.2.1", *_PRODUCT_VERSIONS)
_INVALID_API_URLS = (
    "ftp://ds.example",
    "https://user:secret-token@ds.example",
    "https://ds.example?token=secret-token",
    "https://ds.example#fragment",
)


def _connection() -> ConnectionSettings:
    return ConnectionSettings(
        api_url="https://ds.example/dolphinscheduler", api_token="secret-token"
    )


def _mock_server(
    monkeypatch: pytest.MonkeyPatch,
    responses: list[httpx.Response | httpx.RequestError],
    *,
    expected_timeout: float = 10.0,
) -> list[httpx.Request]:
    requests: list[httpx.Request] = []
    original_client = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        response = responses.pop(0)
        if isinstance(response, httpx.RequestError):
            raise response
        return response

    def client(
        *, headers: Mapping[str, str], timeout: float, follow_redirects: bool
    ) -> httpx.Client:
        assert timeout == expected_timeout
        assert follow_redirects is False
        return original_client(
            headers=headers,
            timeout=timeout,
            follow_redirects=follow_redirects,
            transport=httpx.MockTransport(handler),
        )

    monkeypatch.setattr(httpx, "Client", client)
    return requests


@pytest.mark.parametrize("version", _PRODUCT_VERSIONS)
def test_product_info_discovers_without_a_selected_version(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    requests = _mock_server(
        monkeypatch,
        [
            httpx.Response(
                200, json={"code": 0, "msg": "成功", "data": {"version": version}}
            )
        ],
    )
    assert discovery.discover_version(_connection()) == discovery.DiscoveredVersion(
        version, "product_info"
    )
    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert (
        str(requests[0].url)
        == "https://ds.example/dolphinscheduler/ui-plugins/query-product-info"
    )
    assert requests[0].headers["token"] == "secret-token"


@pytest.mark.parametrize("version", _OPENAPI_VERSIONS)
@pytest.mark.parametrize("missing_status", [404, 405])
def test_absent_product_info_uses_openapi(
    monkeypatch: pytest.MonkeyPatch, version: str, missing_status: int
) -> None:
    requests = _mock_server(
        monkeypatch,
        [
            httpx.Response(missing_status),
            httpx.Response(
                200, json={"openapi": "3.0.1", "info": {"version": version}}
            ),
        ],
    )
    assert discovery.discover_version(_connection()) == discovery.DiscoveredVersion(
        version, "openapi"
    )
    assert requests[1].url.path == "/dolphinscheduler/v3/api-docs"


@pytest.mark.parametrize(
    "version",
    ["V1", "V2", "unknown", "", "3.4", "3.4.1-SNAPSHOT", " 3.4.1 ", 341, None],
)
def test_non_product_versions_are_rejected(
    monkeypatch: pytest.MonkeyPatch, version: str | int | None
) -> None:
    _mock_server(
        monkeypatch,
        [httpx.Response(404), httpx.Response(200, json={"info": {"version": version}})],
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == "invalid_response"


@pytest.mark.parametrize("version", ["9.9.9", "3.1.9", "1.3.9", "3.3.0", "3.2.2"])
def test_exact_membership_never_selects_a_nearby_version(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    _mock_server(
        monkeypatch,
        [httpx.Response(404), httpx.Response(200, json={"info": {"version": version}})],
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == "unsupported_version"


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (401, "authentication_failed"),
        (403, "authentication_failed"),
        (500, "http_error"),
        (302, "redirect_refused"),
        (307, "redirect_refused"),
    ],
)
def test_terminal_http_failures_do_not_fall_back_or_forward_credentials(
    monkeypatch: pytest.MonkeyPatch, status: int, reason: str
) -> None:
    requests = _mock_server(
        monkeypatch,
        [
            httpx.Response(
                status,
                headers={"location": "https://elsewhere.example/steal"},
                text="secret-token backend details",
            )
        ],
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == reason
    assert len(requests) == 1
    assert error.value.probes == (
        {"source": "product_info", "reason": reason, "http_status": status},
    )
    assert "secret-token" not in str(error.value)
    assert "elsewhere" not in repr(error.value.probes)


def test_transport_failure_is_visible_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = _mock_server(
        monkeypatch, [httpx.ConnectError("secret-token backend detail")]
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == "transport_failed"
    assert len(requests) == 1
    assert "secret-token" not in str(error.value)


@pytest.mark.parametrize(
    "body",
    [
        b"null",
        b"[]",
        b"<html>secret-token</html>",
        b'{"code":false,"data":{"version":"3.4.1"}}',
        b'{"code":1,"data":{"version":"3.4.1"}}',
        b'{"code":0,"data":null}',
    ],
)
def test_malformed_or_unsuccessful_product_envelope_is_rejected(
    monkeypatch: pytest.MonkeyPatch, body: bytes
) -> None:
    requests = _mock_server(monkeypatch, [httpx.Response(200, content=body)])
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == "invalid_response"
    assert len(requests) == 1
    assert "secret-token" not in str(error.value)


def test_absent_probes_report_both_safe_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_server(monkeypatch, [httpx.Response(404), httpx.Response(405)])
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == "unavailable"
    assert error.value.probes == (
        {"source": "product_info", "reason": "unavailable", "http_status": 404},
        {"source": "openapi", "reason": "unavailable", "http_status": 405},
    )


def test_oversized_response_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(discovery, "_MAX_RESPONSE_BYTES", 10)
    _mock_server(monkeypatch, [httpx.Response(200, content=b"x" * 11)])
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == "response_too_large"


@pytest.mark.parametrize("url", _INVALID_API_URLS)
def test_connection_settings_reject_invalid_url_without_exposing_secrets(
    monkeypatch: pytest.MonkeyPatch, url: str
) -> None:
    requests = _mock_server(monkeypatch, [])
    with pytest.raises(ConfigError) as error:
        ConnectionSettings(api_url=url, api_token="secret-token")
    assert error.value.details == {"key": "DS_API_URL"}
    assert "secret-token" not in str(error.value.to_payload())
    assert requests == []


@pytest.mark.parametrize("url", _INVALID_API_URLS)
def test_invalid_connection_fails_before_requests(
    monkeypatch: pytest.MonkeyPatch, url: str
) -> None:
    requests = _mock_server(monkeypatch, [])
    # Even callers bypassing model validation must not send credentials to unsafe URLs.
    unsafe_connection = ConnectionSettings.model_construct(
        api_url=url, api_token="secret-token"
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(unsafe_connection)
    assert error.value.reason == "invalid_connection"
    assert "secret-token" not in str(error.value)
    assert "secret-token" not in str(error.value.probes)
    assert requests == []


@pytest.mark.parametrize("version", ["3.2.0", "3.2.1"])
def test_legacy_route_collision_allows_source_backed_openapi_fallback(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    requests = _mock_server(
        monkeypatch,
        [
            httpx.Response(
                200, json={"code": 110003, "msg": "query plugins error", "data": None}
            ),
            httpx.Response(200, json={"info": {"version": version}}),
        ],
    )
    assert discovery.discover_version(_connection()) == discovery.DiscoveredVersion(
        version, "openapi"
    )
    assert len(requests) == 2
    assert requests[1].extensions["timeout"] == {
        "connect": 10.0,
        "read": 10.0,
        "write": 10.0,
        "pool": 10.0,
    }


def test_configured_timeout_applies_to_each_discovery_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = _mock_server(
        monkeypatch,
        [httpx.Response(200, json={"code": 0, "data": {"version": "3.4.1"}})],
        expected_timeout=2.25,
    )
    connection = ConnectionSettings(
        api_url="https://ds.example/dolphinscheduler",
        api_token="secret-token",
        api_timeout_seconds=2.25,
    )

    assert discovery.discover_version(connection).version == "3.4.1"
    assert requests[0].extensions["timeout"] == {
        "connect": 2.25,
        "read": 2.25,
        "write": 2.25,
        "pool": 2.25,
    }


def test_discovery_total_budget_caps_a_larger_request_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(discovery, "_DISCOVERY_TIMEOUT_SECONDS", 1.5)
    requests = _mock_server(
        monkeypatch,
        [httpx.Response(200, json={"code": 0, "data": {"version": "3.4.1"}})],
        expected_timeout=20.0,
    )
    connection = ConnectionSettings(
        api_url="https://ds.example/dolphinscheduler",
        api_token="secret-token",
        api_timeout_seconds=20.0,
    )

    assert discovery.discover_version(connection).version == "3.4.1"
    applied = requests[0].extensions["timeout"]
    assert all(0 < value <= 1.5 for value in applied.values())


def test_stock_322_database_version_is_not_aliased(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mock_server(
        monkeypatch,
        [
            httpx.Response(200, json={"code": 110003, "data": None}),
            httpx.Response(200, json={"info": {"version": "3.3.0"}}),
        ],
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == "unsupported_version"
    assert error.value.probes[0]["reason"] == "legacy_route_collision"


@pytest.mark.parametrize("code", [110002, 110004, "110003"])
def test_other_business_errors_are_not_route_misses(
    monkeypatch: pytest.MonkeyPatch, code: int | str
) -> None:
    requests = _mock_server(
        monkeypatch, [httpx.Response(200, json={"code": code, "data": None})]
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == "invalid_response"
    assert len(requests) == 1


def test_collision_code_with_data_is_not_silently_discarded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = _mock_server(
        monkeypatch,
        [httpx.Response(200, json={"code": 110003, "data": {"version": "3.4.1"}})],
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())
    assert error.value.reason == "invalid_response"
    assert len(requests) == 1


@pytest.mark.parametrize(
    "version",
    [
        "2.0.1",
        "2.0.2",
        "2.0.3",
        "2.0.4",
        "2.0.5",
        "2.0.6",
        "2.0.7",
        "2.0.8",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
    ],
)
@pytest.mark.parametrize("probe", ["product_info", "openapi"])
def test_intermediate_exact_profiles_do_not_imply_discovery_support(
    monkeypatch: pytest.MonkeyPatch, version: str, probe: str
) -> None:
    responses: list[httpx.Response | httpx.RequestError] = (
        [httpx.Response(200, json={"code": 0, "data": {"version": version}})]
        if probe == "product_info"
        else [
            httpx.Response(404),
            httpx.Response(200, json={"info": {"version": version}}),
        ]
    )
    _mock_server(monkeypatch, responses)

    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_version(_connection())

    assert error.value.reason == "unsupported_version"


def _document_probe_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        discovery_facts,
        "DOCUMENT_PROBES",
        tuple(
            SimpleNamespace(source=source, path=path)
            for source, path in (
                ("openapi3", "v3/api-docs"),
                ("swagger2", "v2/api-docs"),
                ("openapi3", "v3/api-docs?group=v1(current)"),
                ("openapi3", "v3/api-docs?group=v2"),
            )
        ),
        raising=False,
    )


@pytest.mark.parametrize("version", _PRODUCT_VERSIONS)
def test_target_exact_metadata_short_circuits_documents(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    requests = _mock_server(
        monkeypatch,
        [httpx.Response(200, json={"code": 0, "data": {"version": version}})],
    )
    result = discovery.discover_target(_connection())
    assert result.version == version
    assert result.reported_version == version
    assert result.candidate_versions == ()
    assert len(requests) == 1


def test_target_reuses_openapi_document_and_fetches_fixed_group_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _document_probe_paths(monkeypatch)
    swagger = {"swagger": "2.0", "paths": {"/projects": {"get": {}}}}
    openapi = {
        "openapi": "3.0.1",
        "info": {"version": "V1"},
        "paths": {"/projects": {"get": {}}},
    }
    requests = _mock_server(
        monkeypatch,
        [
            httpx.Response(404),
            httpx.Response(200, json=openapi),
            httpx.Response(200, json=swagger),
            httpx.Response(404),
            httpx.Response(404),
        ],
    )
    observed: list[dict[str, JsonValue]] = []

    def match(payloads: dict[str, JsonValue]) -> ContractMatch:
        observed.append(payloads)
        return ContractMatch(("1.3.9", "2.0.0"), (), "Candidates only.")

    monkeypatch.setattr(discovery, "match_documents", match)
    result = discovery.discover_target(_connection())
    assert result.version is None
    assert result.source == "contract"
    assert result.candidate_versions == ("1.3.9", "2.0.0")
    assert observed[0]["v3/api-docs"] == openapi
    assert observed[0]["v2/api-docs"] == swagger
    assert len(requests) == 5
    assert (
        sum(
            request.url.path.endswith("/v3/api-docs") and not request.url.query
            for request in requests
        )
        == 1
    )
    assert [request.url.params.get("group") for request in requests[-2:]] == [
        "v1(current)",
        "v2",
    ]
    assert all(request.url.host == "ds.example" for request in requests)


@pytest.mark.parametrize("version", ["8.0.0", "9.9.9", "1.3.9"])
def test_unknown_product_metadata_preserved_without_contract_alias(
    monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    requests = _mock_server(
        monkeypatch,
        [httpx.Response(404), httpx.Response(200, json={"info": {"version": version}})],
    )
    result = discovery.discover_target(_connection())
    assert result.version is None
    assert result.reported_version == version
    assert result.candidate_versions == ()
    assert len(requests) == 2


def test_330_database_anomaly_is_retained_as_metadata_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _document_probe_paths(monkeypatch)
    requests = _mock_server(
        monkeypatch,
        [
            httpx.Response(200, json={"code": 110003, "data": None}),
            httpx.Response(200, json={"info": {"version": "3.3.0"}}),
            httpx.Response(404),
            httpx.Response(404),
            httpx.Response(404),
        ],
    )
    monkeypatch.setattr(
        discovery,
        "match_documents",
        lambda payloads: ContractMatch(("3.2.2",), (), "Candidate only."),
    )
    result = discovery.discover_target(_connection())
    assert result.version is None
    assert result.reported_version == "3.3.0"
    assert result.candidate_versions == ("3.2.2",)
    assert len(requests) == 5


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (401, "authentication_failed"),
        (403, "authentication_failed"),
        (302, "redirect_refused"),
        (500, "http_error"),
    ],
)
def test_document_probe_http_failures_never_disguise_version_absence(
    monkeypatch: pytest.MonkeyPatch, status: int, reason: str
) -> None:
    _document_probe_paths(monkeypatch)
    requests = _mock_server(
        monkeypatch,
        [
            httpx.Response(404),
            httpx.Response(404),
            httpx.Response(
                status,
                text="private backend contents",
                headers={"location": "https://elsewhere.example/steal"},
            ),
        ],
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_target(_connection())
    assert error.value.reason == reason
    assert len(requests) == 3
    assert "private" not in repr(error.value.probes)
    assert "elsewhere" not in repr(error.value.probes)


def test_document_probe_transport_failure_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _document_probe_paths(monkeypatch)
    requests = _mock_server(
        monkeypatch,
        [httpx.Response(404), httpx.Response(404), httpx.ReadError("private failure")],
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_target(_connection())
    assert error.value.reason == "transport_failed"
    assert len(requests) == 3


def test_document_probe_response_limit_applies_before_json_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _document_probe_paths(monkeypatch)
    monkeypatch.setattr(discovery, "_MAX_RESPONSE_BYTES", 10)
    _mock_server(
        monkeypatch,
        [
            httpx.Response(404),
            httpx.Response(404),
            httpx.Response(200, content=b"x" * 11),
        ],
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_target(_connection())
    assert error.value.reason == "response_too_large"


def test_target_total_deadline_prevents_new_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(discovery, "_DISCOVERY_TIMEOUT_SECONDS", 0.0)
    requests = _mock_server(monkeypatch, [])
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_target(_connection())
    assert error.value.reason == "transport_failed"
    assert requests == []


def test_deep_json_response_is_rejected_safely(monkeypatch: pytest.MonkeyPatch) -> None:
    _mock_server(
        monkeypatch, [httpx.Response(200, content=b"[" * 2000 + b"0" + b"]" * 2000)]
    )
    with pytest.raises(discovery.VersionDiscoveryError) as error:
        discovery.discover_target(_connection())
    assert error.value.reason == "invalid_response"


def test_conflicting_fetched_metadata_never_selects_exact_or_candidate_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests = _mock_server(
        monkeypatch,
        [
            httpx.Response(200, json={"code": 0, "data": {"version": "3.3.0"}}),
            httpx.Response(
                200, json={"openapi": "3.0.1", "info": {"version": "3.4.1"}}
            ),
        ],
    )
    result = discovery.discover_target(_connection())
    assert result.version is None
    assert result.candidate_versions == ()
    assert result.reported_version == "3.3.0"
    assert {probe.get("reported_version") for probe in result.probes} >= {
        "3.3.0",
        "3.4.1",
    }
    assert result.probes[-1]["reason"] == "conflicting_versions"
    assert len(requests) == 2

from __future__ import annotations

import json
from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError, UnsupportedFeatureError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.alert_plugins import (
    AlertPluginAdapter,
    AlertPluginDomain,
)
from tests.support import make_profile

_PLUGIN_VERSIONS = TARGET_DS_VERSIONS[TARGET_DS_VERSIONS.index("2.0.0") :]
_ENTITY_RESULT_VERSIONS = frozenset(
    {
        "3.0.0",
        "3.0.1",
        "3.0.2",
        "3.0.3",
        "3.0.4",
        "3.0.5",
        "3.0.6",
        "3.1.0",
        "3.1.1",
        "3.1.2",
        "3.1.3",
        "3.1.4",
        "3.1.5",
        "3.1.6",
        "3.1.7",
        "3.1.8",
        "3.1.9",
        "3.2.0",
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)
_TEST_SEND_VERSIONS = frozenset(
    {
        "3.2.1",
        "3.2.2",
        "3.3.1",
        "3.3.2",
        "3.4.0",
        "3.4.1",
        "3.4.2",
        "3.4.3",
    }
)
_TRANSIENT_VERSIONS = frozenset({"3.2.1", "3.2.2"})


def test_ds_139_alert_plugin_absence_rejects_without_network_requests() -> None:
    profile = make_profile(ds_version="1.3.9")
    requests_seen = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = AlertPluginAdapter.for_version("1.3.9")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        with pytest.raises(UnsupportedFeatureError):
            domain.alert_plugins.list(page_no=1, page_size=20)
        with pytest.raises(UnsupportedFeatureError):
            domain.ui_plugins.list(plugin_type="ALERT")
        with pytest.raises(UnsupportedFeatureError):
            domain.alert_plugins.test_send(
                plugin_define_id=3,
                plugin_instance_params="[]",
            )

    assert requests_seen == 0


@pytest.mark.parametrize(
    "ds_version",
    ["3.2.1", "3.2.2", "3.3.1", "3.3.2", "3.4.0", "3.4.1", "3.4.2", "3.4.3"],
)
def test_delivery_instance_page_rejects_null_native_payload(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request.url.path)
        return _success(None)

    adapter = AlertPluginAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        with pytest.raises(ApiTransportError, match="generated API contract"):
            domain.alert_plugins.list_all()

    assert requests_seen == ["/dolphinscheduler/alert-plugin-instances"]


@pytest.mark.parametrize("ds_version", ["2.0.0", "3.0.0", "3.1.0", "3.1.7"])
def test_legacy_instance_list_normalizes_source_proven_empty_page(
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    requests_seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request.url.path)
        if request.url.path == "/dolphinscheduler/alert-plugin-instances":
            return _success(
                {
                    "totalList": None,
                    "total": 0,
                    "totalPage": 0,
                    "currentPage": 1,
                }
            )
        # DS 2.0.x-3.2.0 returns dataList:null here when there are no instances.
        return _success(None)

    adapter = AlertPluginAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        assert domain.alert_plugins.list_all() == []

    assert requests_seen == ["/dolphinscheduler/alert-plugin-instances"]


@pytest.mark.parametrize("ds_version", ["3.1.8", "3.2.0"])
def test_fixed_instance_page_rejects_null_native_total_list(ds_version: str) -> None:
    profile = make_profile(ds_version=ds_version)
    payload = {"totalList": None, "total": 0, "currentPage": 1}
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )
    with http_client, pytest.raises(ApiTransportError, match="generated API contract"):
        AlertPluginAdapter.for_version(ds_version).bind(
            profile,
            http_client=http_client,
        ).alert_plugins.list_all()


@pytest.mark.parametrize(
    "payload",
    [
        {"total": 0, "currentPage": 1},
        {"totalList": "not a list", "total": 0, "currentPage": 1},
    ],
)
def test_nullable_instance_page_does_not_hide_missing_or_malformed_list(
    payload: object,
) -> None:
    profile = make_profile(ds_version="3.1.0")
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )
    with http_client, pytest.raises(ApiTransportError, match="generated API contract"):
        AlertPluginAdapter.for_version("3.1.0").bind(
            profile,
            http_client=http_client,
        ).alert_plugins.list_all()


def test_nullable_instance_page_rejects_null_when_requested_page_should_have_rows() -> (
    None
):
    profile = make_profile(ds_version="3.1.0")
    payload = {
        "totalList": None,
        "total": 101,
        "totalPage": 2,
        "currentPage": 2,
    }
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )
    with http_client, pytest.raises(ApiTransportError) as error_info:
        AlertPluginAdapter.for_version("3.1.0").bind(
            profile,
            http_client=http_client,
        ).alert_plugins.list(page_no=2, page_size=100)
    assert error_info.value.details["field"] == "totalList"
    assert (
        error_info.value.details["reason"]
        == "null page items contradict the reported total"
    )


def test_nullable_instance_page_accepts_null_beyond_last_row() -> None:
    profile = make_profile(ds_version="3.1.0")
    payload = {
        "totalList": None,
        "total": 100,
        "totalPage": 1,
        "currentPage": 2,
    }
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )
    with http_client:
        page = (
            AlertPluginAdapter.for_version("3.1.0")
            .bind(
                profile,
                http_client=http_client,
            )
            .alert_plugins.list(page_no=2, page_size=100)
        )
    assert page.totalList == []
    assert page.total == 100


@pytest.mark.parametrize("ds_version", _PLUGIN_VERSIONS)
def test_alert_plugin_domain_executes_the_exact_reviewed_recipe(  # noqa: C901
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    current: dict[str, object] | None = _plugin(
        plugin_id=7,
        name="slack-old",
        params="[]",
        modern=ds_version in _TEST_SEND_VERSIONS,
    )
    requests_seen: list[
        tuple[str, str, dict[str, list[str]], dict[str, list[str]]]
    ] = []

    def handler(request: httpx.Request) -> httpx.Response:  # noqa: C901
        nonlocal current
        query = parse_qs(request.url.query.decode(), keep_blank_values=True)
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        requests_seen.append((request.method, request.url.path, query, form))
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/ui-plugins/query-by-type"
        ):
            assert query == {"pluginType": ["ALERT"]}
            return _success([_definition()])
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/ui-plugins/3"
        ):
            return _success(_definition())
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/alert-plugin-instances"
        ):
            items = [] if current is None else [current]
            search = query.get("searchVal", [None])[0]
            if search is not None:
                items = [
                    item
                    for item in items
                    if search.casefold() in str(item["instanceName"]).casefold()
                ]
            return _success(
                {
                    "totalList": items,
                    "total": len(items),
                    "totalPage": 0 if not items else 1,
                    "currentPage": int(query["pageNo"][0]),
                }
            )
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/alert-plugin-instances/list"
        ):
            return _success([] if current is None else [current])
        if (
            request.method == "GET"
            and request.url.path == "/dolphinscheduler/alert-plugin-instances/8"
        ):
            return _success(current)
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/alert-plugin-instances"
        ):
            current = _plugin_from_form(
                form,
                plugin_id=8,
                modern=ds_version in _TEST_SEND_VERSIONS,
                transient=ds_version in _TRANSIENT_VERSIONS,
            )
            result = current if ds_version in _ENTITY_RESULT_VERSIONS else None
            return _success(result)
        if (
            request.method == "PUT"
            and request.url.path == "/dolphinscheduler/alert-plugin-instances/8"
        ):
            assert current is not None
            current = {
                **current,
                "instanceName": form["instanceName"][0],
                "pluginInstanceParams": form["pluginInstanceParams"][0],
            }
            if "warningType" in form:
                current["warningType"] = form["warningType"][0]
            result = current if ds_version in _TEST_SEND_VERSIONS else None
            return _success(result)
        if (
            request.method == "POST"
            and request.url.path == "/dolphinscheduler/alert-plugin-instances/test-send"
        ):
            return _success(True)
        if (
            request.method == "DELETE"
            and request.url.path == "/dolphinscheduler/alert-plugin-instances/8"
        ):
            current = None
            delete_result: object = True if ds_version in _TEST_SEND_VERSIONS else None
            return _success(delete_result)
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = AlertPluginAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        assert isinstance(domain, AlertPluginDomain)
        definitions = domain.ui_plugins.list(plugin_type="ALERT")
        definition = domain.ui_plugins.get(plugin_id=3)
        assert definitions[0].pluginName == "Slack"
        assert definition.pluginParams == "[]"
        page = domain.alert_plugins.list(
            page_no=1,
            page_size=20,
            search="slack",
        )
        assert page.total == 1
        created = domain.alert_plugins.create(
            plugin_define_id=3,
            instance_name="slack-ops",
            plugin_instance_params=(
                '[{"field":"url","type":"input","title":"URL","value":"ops"}]'
            ),
        )
        assert created.id == 8
        if ds_version in _TRANSIENT_VERSIONS:
            assert current is not None
            current["warningType"] = "FAILURE"
        updated = domain.alert_plugins.update(
            alert_plugin_id=8,
            instance_name="slack-platform",
            plugin_instance_params=(
                '[{"field":"url","type":"input","title":"URL","value":"platform"}]'
            ),
        )
        assert updated.instanceName == "slack-platform"
        request_count = len(requests_seen)
        if ds_version in _TEST_SEND_VERSIONS:
            assert (
                domain.alert_plugins.test_send(
                    plugin_define_id=3,
                    plugin_instance_params="[]",
                )
                is True
            )
        else:
            with pytest.raises(UnsupportedFeatureError):
                domain.alert_plugins.test_send(
                    plugin_define_id=3,
                    plugin_instance_params="[]",
                )
            assert len(requests_seen) == request_count
        assert domain.alert_plugins.delete(alert_plugin_id=8) is True

    page_request = next(
        item
        for item in requests_seen
        if item[0] == "GET" and item[1] == "/dolphinscheduler/alert-plugin-instances"
    )
    if ds_version == "2.0.0":
        assert "searchVal" not in page_request[2]
    else:
        assert page_request[2]["searchVal"] == ["slack"]
    create_request = next(
        item
        for item in requests_seen
        if item[0] == "POST" and item[1] == "/dolphinscheduler/alert-plugin-instances"
    )
    if ds_version in _TRANSIENT_VERSIONS:
        assert create_request[3]["instanceType"] == ["NORMAL"]
        assert create_request[3]["warningType"] == ["ALL"]
        update_request = next(item for item in requests_seen if item[0] == "PUT")
        assert update_request[3]["warningType"] == ["FAILURE"]
    else:
        assert "instanceType" not in create_request[3]
        assert "warningType" not in create_request[3]

    plugin_path = "/dolphinscheduler/alert-plugin-instances"
    expected_requests = [
        ("GET", "/dolphinscheduler/ui-plugins/query-by-type"),
        ("GET", "/dolphinscheduler/ui-plugins/3"),
        ("GET", plugin_path),
        ("POST", plugin_path),
        ("GET", plugin_path),
    ]
    if ds_version in _TRANSIENT_VERSIONS:
        expected_requests.append(("GET", f"{plugin_path}/8"))
    expected_requests.extend([("PUT", f"{plugin_path}/8"), ("GET", plugin_path)])
    if ds_version in _TEST_SEND_VERSIONS:
        expected_requests.append(("POST", f"{plugin_path}/test-send"))
    expected_requests.extend([("DELETE", f"{plugin_path}/8"), ("GET", plugin_path)])
    assert [(method, path) for method, path, _query, _form in requests_seen] == (
        expected_requests
    )


@pytest.mark.parametrize("ds_version", ["2.0.0", "3.0.0", "3.4.3"])
@pytest.mark.parametrize("operation", ["create", "rename"])
def test_alert_plugin_empty_params_follow_upstream_null_projection(
    ds_version: str,
    operation: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    current: dict[str, object] | None = (
        None
        if operation == "create"
        else _plugin(plugin_id=8, name="script-old", params="null", modern=True)
    )
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        requests_seen.append((request.method, request.url.path))
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        if request.method in {"POST", "PUT"}:
            assert form["pluginInstanceParams"] == ["[]"]
            current = _plugin(
                plugin_id=8,
                name=form["instanceName"][0],
                params="null",
                modern=True,
            )
            if request.method == "POST":
                result: object = (
                    current if ds_version in _ENTITY_RESULT_VERSIONS else None
                )
            else:
                result = current if ds_version in _TEST_SEND_VERSIONS else None
            return _success(result)
        if request.method == "GET":
            return _success(_page([current]))
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        plugins = (
            AlertPluginAdapter.for_version(ds_version)
            .bind(profile, http_client=client)
            .alert_plugins
        )
        if operation == "create":
            result = plugins.create(
                plugin_define_id=3,
                instance_name="script-new",
                plugin_instance_params="[]",
            )
        else:
            result = plugins.update(
                alert_plugin_id=8,
                instance_name="script-renamed",
                plugin_instance_params="null",
            )

    assert result.pluginInstanceParams == "null"
    assert requests_seen[-1] == (
        "GET",
        "/dolphinscheduler/alert-plugin-instances",
    )


def test_alert_plugin_readback_compares_persisted_field_values() -> None:
    profile = make_profile(ds_version="3.4.3")
    requested = json.dumps(
        [
            {"field": "url", "type": "input", "title": "URL", "value": "old"},
            {"field": "port", "type": "input-number", "title": "Port", "value": 25},
            {
                "field": "recipients",
                "type": "input",
                "title": "",
                "value": ["ops", 2, True],
            },
            {
                "field": "headers",
                "type": "input",
                "title": "Headers",
                "value": {"priority": 1, "active": False},
            },
            {
                "field": "metrics",
                "type": "input",
                "title": "Metrics",
                "value": [0.25, {"ratio": 1.2e20}],
            },
            {"field": "url", "type": "input", "title": "URL", "value": "new"},
        ],
        indent=2,
    )
    readback = json.dumps(
        [
            {
                "name": "port",
                "field": "port",
                "type": "input-number",
                "title": "Port",
                "value": "25",
            },
            {
                "field": "recipients",
                "type": "input",
                "title": "",
                "value": "[ops, 2, true]",
            },
            {
                "field": "headers",
                "type": "input",
                "title": "Headers",
                "value": "{priority=1, active=false}",
            },
            {
                "field": "metrics",
                "type": "input",
                "title": "Metrics",
                "value": "[0.250, {ratio=1.2E20}]",
            },
            {
                "field": "url",
                "type": "input",
                "title": "URL",
                "value": "new",
            },
            {
                "field": "template-only",
                "type": "input",
                "title": "Template only",
                "value": None,
            },
        ],
        separators=(",", ":"),
    )
    current = _plugin(plugin_id=8, name="http", params=readback, modern=True)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return _success(current)
        return _success(_page([current]))

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        created = (
            AlertPluginAdapter.for_version("3.4.3")
            .bind(profile, http_client=client)
            .alert_plugins.create(
                plugin_define_id=3,
                instance_name="http",
                plugin_instance_params=requested,
            )
        )

    assert created.pluginInstanceParams == readback


@pytest.mark.parametrize("missing", ["field", "type", "title"])
def test_alert_plugin_rejects_missing_requested_ui_fields_before_dispatch(
    missing: str,
) -> None:
    profile = make_profile(ds_version="3.4.3")
    requests_seen = 0
    item = {
        "field": "url",
        "type": "input",
        "title": "URL",
        "value": "missing-field",
    }
    item.pop(missing)

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(None)

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as captured,
    ):
        AlertPluginAdapter.for_version("3.4.3").bind(
            profile, http_client=client
        ).alert_plugins.create(
            plugin_define_id=3,
            instance_name="http",
            plugin_instance_params=json.dumps([item]),
        )

    assert requests_seen == 0
    assert "mutation_applied" not in captured.value.details
    assert captured.value.details["field"] == "pluginInstanceParams"


@pytest.mark.parametrize(
    ("requested", "readback"),
    [
        (
            '[{"field":"url","type":"input","title":"URL","value":"expected"}]',
            '[{"field":"url","type":"input","title":"URL","value":"changed"}]',
        ),
        (
            '[{"field":"url","type":"input","title":"URL","value":"expected"}]',
            "not-json",
        ),
        (
            '[{"field":"url","type":"input","title":"URL","value":"expected"}]',
            '[{"value":"expected"}]',
        ),
        (
            '[{"field":"url","type":"input","title":"URL","value":"expected"}]',
            '[{"field":"url","type":"input","title":"URL",'
            '"value":"expected"},{"field":"url","type":"input",'
            '"title":"URL","value":"expected"}]',
        ),
        (
            '[{"field":"optional","type":"input","title":"Optional","value":null}]',
            "null",
        ),
        (
            '[{"field":"metrics","type":"input","title":"Metrics",'
            '"value":[0.25,{"ratio":1.2e20}]}]',
            '[{"field":"metrics","type":"input","title":"Metrics",'
            '"value":"[0.26, {ratio=1.2E20}]"}]',
        ),
    ],
)
def test_alert_plugin_readback_rejects_changed_or_malformed_params(
    requested: str,
    readback: str,
) -> None:
    profile = make_profile(ds_version="3.4.3")
    current = _plugin(plugin_id=8, name="http", params=readback, modern=True)
    requests_seen = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(current if request.method == "POST" else _page([current]))

    with (
        DolphinSchedulerClient(
            profile, transport=httpx.MockTransport(handler)
        ) as client,
        pytest.raises(ApiTransportError) as captured,
    ):
        AlertPluginAdapter.for_version("3.4.3").bind(
            profile, http_client=client
        ).alert_plugins.create(
            plugin_define_id=3,
            instance_name="http",
            plugin_instance_params=requested,
        )

    assert requests_seen == 2
    assert captured.value.details["phase"] == "readback"
    assert captured.value.details["mutation_applied"] is True


@pytest.mark.parametrize(
    ("operation", "ds_version", "method", "suffix"),
    [
        ("create", "3.4.1", "POST", ""),
        ("update", "3.2.1", "PUT", "/8"),
        ("delete", "3.4.1", "DELETE", "/8"),
        ("test", "3.4.1", "POST", "/test-send"),
    ],
)
def test_alert_plugin_failed_mutation_is_once_only_without_readback(
    operation: str,
    ds_version: str,
    method: str,
    suffix: str,
) -> None:
    profile = make_profile(ds_version=ds_version).model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests_seen: list[tuple[str, str]] = []
    plugin_path = "/dolphinscheduler/alert-plugin-instances"

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if request.method == "GET":
            return _success(
                _plugin(plugin_id=8, name="slack", params="[]", modern=True)
            )
        return httpx.Response(503)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = AlertPluginAdapter.for_version(ds_version).bind(
            profile, http_client=client
        )

        def mutate() -> None:
            if operation == "create":
                domain.alert_plugins.create(
                    plugin_define_id=3,
                    instance_name="slack",
                    plugin_instance_params="[]",
                )
            elif operation == "update":
                domain.alert_plugins.update(
                    alert_plugin_id=8,
                    instance_name="slack",
                    plugin_instance_params="[]",
                )
            elif operation == "delete":
                domain.alert_plugins.delete(alert_plugin_id=8)
            else:
                domain.alert_plugins.test_send(
                    plugin_define_id=3, plugin_instance_params="[]"
                )

        with pytest.raises(ApiTransportError) as captured:
            mutate()

    expected = [("GET", f"{plugin_path}/8")] if operation == "update" else []
    assert requests_seen == [*expected, (method, f"{plugin_path}{suffix}")]
    assert captured.value.details["phase"] == "mutation_request"
    assert captured.value.details["mutation_may_have_applied"] is True


def test_alert_plugin_readback_failure_does_not_repeat_the_mutation() -> None:
    profile = make_profile().model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        if request.method == "POST":
            return _success(
                _plugin(plugin_id=8, name="slack", params="[]", modern=True)
            )
        return _success(_page([]))

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        domain = AlertPluginAdapter.for_version(profile.ds_version).bind(
            profile, http_client=client
        )
        with pytest.raises(ApiTransportError) as captured:
            domain.alert_plugins.create(
                plugin_define_id=3,
                instance_name="slack",
                plugin_instance_params="[]",
            )

    assert requests_seen == [
        ("POST", "/dolphinscheduler/alert-plugin-instances"),
        ("GET", "/dolphinscheduler/alert-plugin-instances"),
    ]
    assert captured.value.details["phase"] == "readback"
    assert captured.value.details["mutation_applied"] is True
    assert "do not blindly repeat" in (captured.value.suggestion or "")


@pytest.mark.parametrize("operation", ["test", "delete"])
def test_alert_plugin_false_result_keeps_the_operation_specific_meaning(
    operation: str,
) -> None:
    profile = make_profile()
    requests_seen: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append((request.method, request.url.path))
        return _success(False)

    with DolphinSchedulerClient(
        profile, transport=httpx.MockTransport(handler)
    ) as client:
        plugins = (
            AlertPluginAdapter.for_version(profile.ds_version)
            .bind(profile, http_client=client)
            .alert_plugins
        )
        if operation == "delete":
            with pytest.raises(ApiTransportError) as captured:
                plugins.delete(alert_plugin_id=8)
            assert captured.value.details["phase"] == "readback"
            assert captured.value.details["mutation_applied"] is True
        else:
            assert (
                plugins.test_send(plugin_define_id=3, plugin_instance_params="[]")
                is False
            )

    expected = (
        ("DELETE", "/dolphinscheduler/alert-plugin-instances/8")
        if operation == "delete"
        else ("POST", "/dolphinscheduler/alert-plugin-instances/test-send")
    )
    assert requests_seen == [expected]


def _definition() -> dict[str, object]:
    return {
        "id": 3,
        "pluginName": "Slack",
        "pluginType": "ALERT",
        "pluginParams": "[]",
        "createTime": None,
        "updateTime": None,
    }


def _plugin(
    *,
    plugin_id: int,
    name: str,
    params: str,
    modern: bool,
) -> dict[str, object]:
    data: dict[str, object] = {
        "id": plugin_id,
        "pluginDefineId": 3,
        "instanceName": name,
        "pluginInstanceParams": params,
        "createTime": None,
        "updateTime": None,
        "alertPluginName": "Slack",
    }
    if modern:
        data.update({"instanceType": "NORMAL", "warningType": "ALL"})
    return data


def _plugin_from_form(
    form: dict[str, list[str]],
    *,
    plugin_id: int,
    modern: bool,
    transient: bool,
) -> dict[str, object]:
    data = _plugin(
        plugin_id=plugin_id,
        name=form["instanceName"][0],
        params=form["pluginInstanceParams"][0],
        modern=modern,
    )
    if transient:
        data["instanceType"] = form["instanceType"][0]
        data["warningType"] = form["warningType"][0]
    return data


def _page(items: list[dict[str, object] | None]) -> dict[str, object]:
    return {
        "totalList": items,
        "total": len(items),
        "totalPage": 0 if not items else 1,
        "currentPage": 1,
    }


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})

from __future__ import annotations

import hashlib
import importlib
import json
from contextlib import nullcontext
from copy import deepcopy
from typing import cast
from urllib.parse import parse_qs

import httpx
import pytest
from pydantic import ValidationError

from dsctl.client import DolphinSchedulerClient
from dsctl.errors import ApiTransportError
from dsctl.generated.version_profiles import TARGET_DS_VERSIONS
from dsctl.upstream.alert_groups import (
    _ALERT_GROUP_PROGRAMS,
    AlertGroupAdapter,
    AlertGroupDomain,
    AlertGroupSnapshot,
)
from dsctl.upstream.wire import WireContractError
from tests.support import make_profile

_ENTITY_RESULT_VERSIONS = frozenset(
    {
        "3.0.0",
        "3.0.6",
        "3.1.0",
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
_MODERN_RESULT_VERSIONS = frozenset(
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


@pytest.mark.parametrize("ds_version", TARGET_DS_VERSIONS)
def test_alert_group_domain_executes_the_exact_reviewed_recipe(  # noqa: C901
    ds_version: str,
) -> None:
    profile = make_profile(ds_version=ds_version)
    legacy = ds_version == "1.3.9"
    list_path = (
        "/dolphinscheduler/alert-group/list-paging"
        if legacy
        else "/dolphinscheduler/alert-groups"
    )
    current: dict[str, object] | None = _group(
        group_id=7,
        name="ops",
        description="operations",
        alert_instance_ids="2,3",
        legacy=legacy,
    )
    requests_seen: list[tuple[str, str, dict[str, list[str]]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal current
        form = parse_qs(request.content.decode(), keep_blank_values=True)
        requests_seen.append((request.method, request.url.path, form))
        if request.method == "GET" and request.url.path == list_path:
            items = [] if current is None else [_page_group(ds_version, current)]
            return _success(
                {
                    "totalList": items,
                    "total": len(items),
                    "totalPage": 0 if current is None else 1,
                    "currentPage": 1,
                }
            )
        if (
            not legacy
            and request.method == "POST"
            and request.url.path == "/dolphinscheduler/alert-groups/query"
        ):
            assert form == {"id": [str(current["id"] if current else 0)]}
            return _success(current)
        legacy_create = (
            legacy
            and request.method == "POST"
            and request.url.path == "/dolphinscheduler/alert-group/create"
        )
        modern_create = (
            not legacy and request.method == "POST" and request.url.path == list_path
        )
        if legacy_create or modern_create:
            current = _group_from_form(form, group_id=8, legacy=legacy)
            create_result: object = (
                current
                if ds_version in _ENTITY_RESULT_VERSIONS
                else {"future": "ignored by Void response"}
            )
            return _success(create_result)
        legacy_update = (
            legacy
            and request.method == "POST"
            and request.url.path == "/dolphinscheduler/alert-group/update"
        )
        modern_update = (
            not legacy
            and request.method == "PUT"
            and request.url.path == "/dolphinscheduler/alert-groups/8"
        )
        if legacy_update or modern_update:
            if legacy:
                assert form["id"] == ["8"]
            else:
                assert "id" not in form
                assert not request.url.query
            current = _group_from_form(form, group_id=8, legacy=legacy)
            update_result: object = (
                current
                if ds_version in _MODERN_RESULT_VERSIONS
                else ["ignored", "by", "Void", "response"]
            )
            return _success(update_result)
        legacy_delete = (
            legacy
            and request.method == "POST"
            and request.url.path == "/dolphinscheduler/alert-group/delete"
        )
        modern_delete = (
            not legacy
            and request.method == "DELETE"
            and request.url.path == "/dolphinscheduler/alert-groups/8"
        )
        if legacy_delete or modern_delete:
            if legacy:
                assert form == {"id": ["8"]}
            current = None
            delete_result: object = (
                True
                if ds_version in _MODERN_RESULT_VERSIONS
                else {"ignored": "by Void response"}
            )
            return _success(delete_result)
        message = f"unexpected request {request.method} {request.url.path}"
        raise AssertionError(message)

    adapter = AlertGroupAdapter.for_version(ds_version)
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        domain = adapter.bind(profile, http_client=http_client)
        assert isinstance(domain, AlertGroupDomain)
        page = domain.alert_groups.list(page_no=1, page_size=20, search="ops")
        fetched = domain.alert_groups.get(alert_group_id=7)
        assert page.total == 1
        assert fetched.groupName == "ops"
        if legacy:
            assert cast("AlertGroupSnapshot", fetched).groupType == "EMAIL"
            created = domain.alert_groups.create(
                group_name="platform",
                description="platform alerts",
                alert_instance_ids=None,
                group_type="SMS",
            )
            assert created.id == 8
            assert cast("AlertGroupSnapshot", created).groupType == "SMS"
            updated = domain.alert_groups.update(
                alert_group_id=8,
                group_name="platform-renamed",
                description=None,
                alert_instance_ids=None,
                group_type="EMAIL",
            )
            assert updated.groupName == "platform-renamed"
            assert cast("AlertGroupSnapshot", updated).groupType == "EMAIL"
            assert domain.alert_groups.delete(alert_group_id=8) is True
        else:
            created = domain.alert_groups.create(
                group_name="platform",
                description="platform alerts",
                alert_instance_ids="4,5",
                group_type=None,
            )
            assert created.id == 8
            updated = domain.alert_groups.update(
                alert_group_id=8,
                group_name="platform-renamed",
                description=None,
                alert_instance_ids="5,6",
                group_type=None,
            )
            assert updated.groupName == "platform-renamed"
            assert updated.alertInstanceIds == "5,6"
            assert domain.alert_groups.delete(alert_group_id=8) is True

    if legacy:
        assert any(
            method == "POST"
            and path == "/dolphinscheduler/alert-group/create"
            and form["groupType"] == ["SMS"]
            for method, path, form in requests_seen
        )
        assert any(
            method == "POST"
            and path == "/dolphinscheduler/alert-group/update"
            and form["groupType"] == ["EMAIL"]
            for method, path, form in requests_seen
        )
    else:
        assert any(
            method == "POST"
            and path == "/dolphinscheduler/alert-groups"
            and form["alertInstanceIds"] == ["4,5"]
            for method, path, form in requests_seen
        )
        assert any(
            method == "PUT"
            and path == "/dolphinscheduler/alert-groups/8"
            and form["alertInstanceIds"] == ["5,6"]
            for method, path, form in requests_seen
        )


def test_alert_group_legacy_request_schema_owns_the_alert_type_enum() -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.alert_group")
    model = module.REQUEST_SCHEMAS["create_legacy"].model

    email = model.model_validate({"groupName": "ops", "groupType": "EMAIL"})
    sms = model.model_validate({"groupName": "ops", "groupType": "SMS"})

    assert email.groupType.value == "EMAIL"
    assert sms.groupType.value == "SMS"
    with pytest.raises(ValidationError):
        model.model_validate({"groupName": "ops", "groupType": "FUTURE"})


def test_alert_group_strict_page_schema_cannot_use_the_coercive_epoch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.alert_group")
    schemas = dict(module.RESPONSE_SCHEMAS)
    schemas["page_entity_nullable_id_strict"] = schemas["page_entity_nullable_id"]
    monkeypatch.setattr(module, "RESPONSE_SCHEMAS", schemas)

    with pytest.raises(
        WireContractError,
        match="page_entity_nullable_id_strict response schema is invalid",
    ):
        _ALERT_GROUP_PROGRAMS.fresh_profile("3.1.9")


@pytest.mark.parametrize(
    ("ds_version", "strict"),
    [("3.1.9", True), ("3.2.0", False)],
)
def test_alert_group_page_preserves_exact_integer_validation_epoch(
    ds_version: str,
    *,
    strict: bool,
) -> None:
    profile = make_profile(ds_version=ds_version)
    payload = _page(
        _group(group_id=7, name="ops", description=None, alert_instance_ids="2")
    )
    for field in ("total", "totalPage", "pageSize", "currentPage", "pageNo"):
        payload[field] = str(payload[field])
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )
    context = pytest.raises(ApiTransportError) if strict else nullcontext()
    with http_client, context:
        page = (
            AlertGroupAdapter.for_version(ds_version)
            .bind(profile, http_client=http_client)
            .alert_groups.list(page_no=1, page_size=20)
        )
        assert page.total == 1


@pytest.mark.parametrize(
    ("ds_version", "accepts_null"),
    [("3.0.6", True), ("3.1.0", False)],
)
def test_alert_group_page_preserves_total_list_nullability_epoch(
    ds_version: str,
    *,
    accepts_null: bool,
) -> None:
    profile = make_profile(ds_version=ds_version)
    payload = _page()
    payload["totalList"] = None
    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(lambda _request: _success(payload)),
    )
    context = nullcontext() if accepts_null else pytest.raises(ApiTransportError)
    with http_client, context:
        page = (
            AlertGroupAdapter.for_version(ds_version)
            .bind(profile, http_client=http_client)
            .alert_groups.list(page_no=1, page_size=20)
        )
        assert page.totalList == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("method", "POST"),
        ("path", "alert-groups/{groupId}"),
        ("channel", "form"),
        ("path_fields", []),
        (
            "fields",
            [
                {"name": "id", "binding": "request_param"},
                {"name": "groupName", "binding": "request_param"},
                {"name": "description", "binding": "request_param"},
                {"name": "alertInstanceIds", "binding": "request_param"},
            ],
        ),
    ],
)
def test_alert_group_mixed_path_form_tampering_fails_before_io(
    field: str,
    value: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.alert_group")
    codecs = deepcopy(module.CODECS)
    codecs["update_entity_nullable_id"][field] = value
    monkeypatch.setattr(module, "CODECS", codecs)

    with pytest.raises(WireContractError):
        _ALERT_GROUP_PROGRAMS.fresh_profile("3.4.1")


def test_alert_group_direct_get_absence_tampering_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("dsctl.generated.wire_programs.alert_group")
    profiles = deepcopy(module.PROFILES)
    profiles["1.3.9"]["programs"]["direct_get"] = deepcopy(
        profiles["2.0.0"]["programs"]["direct_get"]
    )
    profiles["1.3.9"]["profile_digest"] = _profile_digest(profiles["1.3.9"])
    monkeypatch.setattr(module, "PROFILES", profiles)

    with pytest.raises(WireContractError, match="programs are incomplete"):
        _ALERT_GROUP_PROGRAMS.fresh_profile("1.3.9")


def test_alert_group_read_retries_and_accepts_a_raw_page() -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 2, "api_retry_backoff_ms": 0}
    )
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        if requests_seen == 1:
            return httpx.Response(503, json={"message": "unavailable"})
        return httpx.Response(
            200,
            json=_page(
                _group(
                    group_id=7,
                    name="ops",
                    description=None,
                    alert_instance_ids="2",
                )
            ),
        )

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client:
        page = (
            AlertGroupAdapter.for_version("3.4.1")
            .bind(profile, http_client=http_client)
            .alert_groups.list(page_no=1, page_size=20)
        )

    assert requests_seen == 2
    assert page.total == 1


def test_alert_group_mutation_requires_an_envelope_and_is_not_retried() -> None:
    profile = make_profile(ds_version="3.4.1").model_copy(
        update={"api_retry_attempts": 3, "api_retry_backoff_ms": 0}
    )
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return httpx.Response(200, json=41)

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError):
        AlertGroupAdapter.for_version("3.4.1").bind(
            profile,
            http_client=http_client,
        ).alert_groups.create(
            group_name="ops",
            description=None,
            alert_instance_ids="2",
            group_type=None,
        )

    assert requests_seen == 1


def test_alert_group_modern_delete_false_fails_before_readback() -> None:
    profile = make_profile(ds_version="3.4.1")
    requests_seen = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests_seen
        requests_seen += 1
        return _success(False)

    http_client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with http_client, pytest.raises(ApiTransportError):
        AlertGroupAdapter.for_version("3.4.1").bind(
            profile,
            http_client=http_client,
        ).alert_groups.delete(alert_group_id=8)

    assert requests_seen == 1


def _group(
    *,
    group_id: int,
    name: str,
    description: str | None,
    alert_instance_ids: str,
    legacy: bool = False,
    group_type: str = "EMAIL",
) -> dict[str, object]:
    data: dict[str, object] = {
        "id": group_id,
        "groupName": name,
        "description": description,
        "createTime": None,
        "updateTime": None,
    }
    if legacy:
        data["groupType"] = group_type
    else:
        data.update({"alertInstanceIds": alert_instance_ids, "createUserId": 1})
    return data


def _group_from_form(
    form: dict[str, list[str]],
    *,
    group_id: int,
    legacy: bool = False,
) -> dict[str, object]:
    return _group(
        group_id=group_id,
        name=form["groupName"][0],
        description=form.get("description", [None])[0],
        alert_instance_ids=("" if legacy else form["alertInstanceIds"][0]),
        legacy=legacy,
        group_type=form.get("groupType", ["EMAIL"])[0],
    )


def _page_group(ds_version: str, current: dict[str, object]) -> dict[str, object]:
    if ds_version != "2.0.0":
        return current
    return {
        key: value
        for key, value in current.items()
        if key in {"id", "groupName", "description", "createTime", "updateTime"}
    }


def _page(item: dict[str, object] | None = None) -> dict[str, object]:
    return {
        "totalList": [] if item is None else [item],
        "total": 0 if item is None else 1,
        "totalPage": 0 if item is None else 1,
        "pageSize": 20,
        "currentPage": 1,
        "pageNo": 1,
    }


def _success(data: object) -> httpx.Response:
    return httpx.Response(200, json={"code": 0, "msg": "success", "data": data})


def _profile_digest(profile: dict[str, object]) -> str:
    encoded = json.dumps(
        {
            "schema_version": 2,
            "status": profile["status"],
            "source": profile["source"],
            "recipe_id": profile["recipe_id"],
            "programs": profile["programs"],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"

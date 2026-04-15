from collections.abc import Mapping, Sequence

import pytest
from tests.fakes import (
    FakeAccessToken,
    FakeAccessTokenAdapter,
    FakeProjectAdapter,
    FakeUser,
    FakeUserAdapter,
    fake_service_runtime,
)
from tests.support import make_profile

from dsctl.errors import ApiResultError, PermissionDeniedError, UserInputError
from dsctl.services import access_token as access_token_service
from dsctl.services import runtime as runtime_service


def _install_access_token_service_fakes(
    monkeypatch: pytest.MonkeyPatch,
    *,
    access_token_adapter: FakeAccessTokenAdapter,
    user_adapter: FakeUserAdapter,
) -> None:
    monkeypatch.setattr(
        runtime_service,
        "open_service_runtime",
        lambda env_file=None: fake_service_runtime(
            FakeProjectAdapter(projects=[]),
            access_token_adapter=access_token_adapter,
            user_adapter=user_adapter,
            profile=make_profile(),
        ),
    )


def _mapping(value: object) -> Mapping[str, object]:
    assert isinstance(value, Mapping)
    return value


def _sequence(value: object) -> Sequence[object]:
    assert isinstance(value, Sequence)
    assert not isinstance(value, (str, bytes, bytearray))
    return value


@pytest.fixture
def fake_user_adapter() -> FakeUserAdapter:
    return FakeUserAdapter(
        users=[
            FakeUser(
                id=1,
                user_name_value="alice",
                email="alice@example.com",
                tenant_id_value=10,
                tenant_code_value="analytics",
            ),
            FakeUser(
                id=2,
                user_name_value="bob",
                email="bob@example.com",
                tenant_id_value=11,
                tenant_code_value="warehouse",
            ),
        ]
    )


@pytest.fixture
def fake_access_token_adapter(
    fake_user_adapter: FakeUserAdapter,
) -> FakeAccessTokenAdapter:
    return FakeAccessTokenAdapter(
        access_tokens=[
            FakeAccessToken(
                id=11,
                user_id_value=1,
                token="token-11",
                expire_time_value="2026-12-31 00:00:00",
                create_time_value="2026-04-11 10:00:00",
                update_time_value="2026-04-11 10:00:00",
                user_name_value="alice",
            ),
            FakeAccessToken(
                id=12,
                user_id_value=2,
                token="token-12",
                expire_time_value="2026-12-31 00:00:00",
                user_name_value="bob",
            ),
        ],
        users=fake_user_adapter.users,
    )


def test_list_access_tokens_result_returns_paginated_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_access_token_adapter: FakeAccessTokenAdapter,
    fake_user_adapter: FakeUserAdapter,
) -> None:
    _install_access_token_service_fakes(
        monkeypatch,
        access_token_adapter=fake_access_token_adapter,
        user_adapter=fake_user_adapter,
    )

    result = access_token_service.list_access_tokens_result(page_size=1)
    data = _mapping(result.data)
    items = _sequence(data["totalList"])

    assert data["total"] == 2
    assert data["pageSize"] == 1
    assert result.resolved == {
        "search": None,
        "page_no": 1,
        "page_size": 1,
        "all": False,
    }
    assert list(items) == [
        {
            "id": 11,
            "userId": 1,
            "token": "token-11",
            "expireTime": "2026-12-31 00:00:00",
            "createTime": "2026-04-11 10:00:00",
            "updateTime": "2026-04-11 10:00:00",
            "userName": "alice",
        }
    ]


def test_list_access_tokens_result_translates_permission_denied(
    monkeypatch: pytest.MonkeyPatch,
    fake_access_token_adapter: FakeAccessTokenAdapter,
    fake_user_adapter: FakeUserAdapter,
) -> None:
    _install_access_token_service_fakes(
        monkeypatch,
        access_token_adapter=fake_access_token_adapter,
        user_adapter=fake_user_adapter,
    )

    def deny_list(*, page_no: int, page_size: int, search: str | None = None) -> object:
        del page_no, page_size, search
        raise ApiResultError(
            result_code=access_token_service.USER_NO_OPERATION_PERM,
            result_message="user has no operation privilege",
        )

    monkeypatch.setattr(fake_access_token_adapter, "list", deny_list)

    with pytest.raises(PermissionDeniedError, match="Access-token list requires"):
        access_token_service.list_access_tokens_result()


def test_get_access_token_result_returns_payload_and_resolved_selection(
    monkeypatch: pytest.MonkeyPatch,
    fake_access_token_adapter: FakeAccessTokenAdapter,
    fake_user_adapter: FakeUserAdapter,
) -> None:
    _install_access_token_service_fakes(
        monkeypatch,
        access_token_adapter=fake_access_token_adapter,
        user_adapter=fake_user_adapter,
    )

    result = access_token_service.get_access_token_result(11)
    data = _mapping(result.data)

    assert result.resolved == {
        "accessToken": {
            "id": 11,
            "userId": 1,
            "userName": "alice",
        }
    }
    assert data["token"] == "token-11"
    assert data["userName"] == "alice"


def test_create_access_token_result_resolves_user_and_returns_created_payload(
    monkeypatch: pytest.MonkeyPatch,
    fake_access_token_adapter: FakeAccessTokenAdapter,
    fake_user_adapter: FakeUserAdapter,
) -> None:
    _install_access_token_service_fakes(
        monkeypatch,
        access_token_adapter=fake_access_token_adapter,
        user_adapter=fake_user_adapter,
    )

    result = access_token_service.create_access_token_result(
        user="alice",
        expire_time="2027-01-01 00:00:00",
    )
    data = _mapping(result.data)

    assert data["id"] == 13
    assert data["userId"] == 1
    assert data["token"] == "auto-token-13"
    assert result.resolved == {
        "user": {
            "id": 1,
            "userName": "alice",
            "email": "alice@example.com",
            "tenantId": 10,
            "tenantCode": "analytics",
            "state": 1,
        }
    }


def test_generate_access_token_result_returns_generated_token(
    monkeypatch: pytest.MonkeyPatch,
    fake_access_token_adapter: FakeAccessTokenAdapter,
    fake_user_adapter: FakeUserAdapter,
) -> None:
    _install_access_token_service_fakes(
        monkeypatch,
        access_token_adapter=fake_access_token_adapter,
        user_adapter=fake_user_adapter,
    )

    result = access_token_service.generate_access_token_result(
        user="bob",
        expire_time="2027-01-01 00:00:00",
    )

    assert result.data == {
        "token": "generated-token-2",
        "userId": 2,
        "expireTime": "2027-01-01 00:00:00",
    }


def test_update_access_token_result_preserves_omitted_fields(
    monkeypatch: pytest.MonkeyPatch,
    fake_access_token_adapter: FakeAccessTokenAdapter,
    fake_user_adapter: FakeUserAdapter,
) -> None:
    _install_access_token_service_fakes(
        monkeypatch,
        access_token_adapter=fake_access_token_adapter,
        user_adapter=fake_user_adapter,
    )

    result = access_token_service.update_access_token_result(
        11,
        expire_time="2027-02-02 00:00:00",
    )
    data = _mapping(result.data)

    assert data["id"] == 11
    assert data["userId"] == 1
    assert data["token"] == "token-11"
    assert data["expireTime"] == "2027-02-02 00:00:00"


def test_update_access_token_result_can_regenerate_token(
    monkeypatch: pytest.MonkeyPatch,
    fake_access_token_adapter: FakeAccessTokenAdapter,
    fake_user_adapter: FakeUserAdapter,
) -> None:
    _install_access_token_service_fakes(
        monkeypatch,
        access_token_adapter=fake_access_token_adapter,
        user_adapter=fake_user_adapter,
    )

    result = access_token_service.update_access_token_result(
        11,
        regenerate_token=True,
    )

    assert _mapping(result.data)["token"] == "regenerated-token-11"


def test_update_access_token_result_requires_change() -> None:
    with pytest.raises(
        UserInputError,
        match="requires at least one field change",
    ) as exc_info:
        access_token_service.update_access_token_result(11)

    assert exc_info.value.suggestion == (
        "Pass at least one update flag such as --user, --expire-time, --token, "
        "or --regenerate-token."
    )


def test_update_access_token_result_rejects_conflicting_token_flags() -> None:
    with pytest.raises(
        UserInputError,
        match="cannot use --token with --regenerate-token",
    ) as exc_info:
        access_token_service.update_access_token_result(
            11,
            token="manual-token",
            regenerate_token=True,
        )

    assert exc_info.value.suggestion == (
        "Choose exactly one token source: pass --token explicitly, or use "
        "--regenerate-token to mint a new token."
    )


def test_create_access_token_result_translates_permission_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_user_adapter: FakeUserAdapter,
) -> None:
    access_token_adapter = FakeAccessTokenAdapter(
        access_tokens=[],
        users=fake_user_adapter.users,
        create_errors_by_user_id={
            1: ApiResultError(
                result_code=30001,
                result_message="user has no operation privilege",
            )
        },
    )
    _install_access_token_service_fakes(
        monkeypatch,
        access_token_adapter=access_token_adapter,
        user_adapter=fake_user_adapter,
    )

    with pytest.raises(
        PermissionDeniedError,
        match="requires additional permissions",
    ):
        access_token_service.create_access_token_result(
            user="alice",
            expire_time="2027-01-01 00:00:00",
        )

from urllib.parse import parse_qs

import httpx
import pytest

from dsctl.client import DolphinSchedulerClient
from dsctl.config import ClusterProfile
from dsctl.errors import ApiHttpError, ApiResultError
from tests.support import TEST_API_TOKEN, make_profile


def _detail_string(error: ApiHttpError | ApiResultError, key: str) -> str:
    value = error.details[key]
    assert isinstance(value, str)
    return value


def _json_object(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def test_healthcheck_uses_token_header() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["token"] == TEST_API_TOKEN
        assert request.headers["accept"] == "application/json"
        assert request.headers["x-request-id"].startswith("dsctl-")
        assert (
            str(request.url) == "http://example.test/dolphinscheduler/actuator/health"
        )
        return httpx.Response(
            200,
            json={"status": "UP"},
        )

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client:
        payload = client.healthcheck()

    assert payload == {"status": "UP"}


def test_get_result_unwraps_success_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == "http://example.test/dolphinscheduler/projects/123"
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": {"code": 123, "name": "demo"}},
        )

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client:
        payload = client.get_result("projects/123")

    assert payload == {"code": 123, "name": "demo"}


def test_get_payload_unwraps_success_data_list_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert (
            str(request.url)
            == "http://example.test/dolphinscheduler/projects/7/workflow-instances/901/tasks"
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "msg": "success",
                "dataList": {
                    "workflowInstanceState": "SUCCESS",
                    "taskList": [{"id": 3001, "name": "extract"}],
                },
            },
        )

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client:
        payload = client.get_payload("projects/7/workflow-instances/901/tasks")

    assert payload == {
        "workflowInstanceState": "SUCCESS",
        "taskList": [{"id": 3001, "name": "extract"}],
    }


def test_get_result_raises_api_result_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"code": 10018, "msg": "project demo not found", "data": None},
        )

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client, pytest.raises(ApiResultError) as exc_info:
        client.get_result("projects/123")

    assert exc_info.value.result_code == 10018
    assert exc_info.value.message == "project demo not found"
    assert _detail_string(exc_info.value, "request_id").startswith("dsctl-")
    assert exc_info.value.details["method"] == "GET"
    assert exc_info.value.details["path"] == "projects/123"


def test_get_result_raises_http_error_for_non_2xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            json={"message": "unauthorized"},
        )

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client, pytest.raises(ApiHttpError) as exc_info:
        client.get_result("projects")

    assert exc_info.value.status_code == 401
    assert _detail_string(exc_info.value, "request_id").startswith("dsctl-")


@pytest.mark.parametrize("status_code", [403, 404])
def test_get_result_raises_http_error_for_other_non_retryable_status_codes(
    status_code: int,
) -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(status_code, json={"message": f"http {status_code}"})

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client, pytest.raises(ApiHttpError) as exc_info:
        client.get_result("projects")

    assert attempts == 1
    assert exc_info.value.status_code == status_code


def test_get_result_retries_http_429_for_read_paths() -> None:
    attempts = 0
    profile = ClusterProfile(
        api_url="http://example.test/dolphinscheduler",
        api_token=TEST_API_TOKEN,
        api_retry_attempts=3,
        api_retry_backoff_ms=0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(
                429,
                json={"message": "rate limited"},
            )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": {"code": 7}},
        )

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        payload = client.get_result("projects/7")

    assert attempts == 2
    assert payload == {"code": 7}


def test_post_payload_returns_raw_json_when_response_has_no_result_envelope() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert (
            str(request.url)
            == "http://example.test/dolphinscheduler/v2/projects/7/task-instances/24"
        )
        return httpx.Response(
            200, json={"id": 24, "name": "stream-task", "taskExecuteType": "STREAM"}
        )

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client:
        payload = client.post_payload("v2/projects/7/task-instances/24")

    assert _json_object(payload)["id"] == 24


def test_get_result_retries_transport_errors_for_read_paths() -> None:
    attempts = 0
    profile = ClusterProfile(
        api_url="http://example.test/dolphinscheduler",
        api_token=TEST_API_TOKEN,
        api_retry_attempts=3,
        api_retry_backoff_ms=0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            message = "timed out"
            raise httpx.ReadTimeout(message, request=request)
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": {"code": 7}},
        )

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        payload = client.get_result("projects/7")

    assert attempts == 2
    assert payload == {"code": 7}


def test_get_result_retries_http_503_for_read_paths() -> None:
    attempts = 0
    profile = ClusterProfile(
        api_url="http://example.test/dolphinscheduler",
        api_token=TEST_API_TOKEN,
        api_retry_attempts=3,
        api_retry_backoff_ms=0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(
                503,
                json={"message": "temporarily unavailable"},
            )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": {"code": 7}},
        )

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        payload = client.get_result("projects/7")

    assert attempts == 2
    assert payload == {"code": 7}


def test_post_result_does_not_retry_by_default_and_reports_request_context() -> None:
    attempts = 0
    profile = ClusterProfile(
        api_url="http://example.test/dolphinscheduler",
        api_token=TEST_API_TOKEN,
        api_retry_attempts=3,
        api_retry_backoff_ms=0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(
            503,
            json={"message": "temporarily unavailable"},
        )

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client, pytest.raises(ApiHttpError) as exc_info:
        client.post_result("v2/projects", json_body={"name": "demo"})

    assert attempts == 1
    assert exc_info.value.status_code == 503
    assert exc_info.value.details["method"] == "POST"
    assert exc_info.value.details["path"] == "v2/projects"
    assert exc_info.value.details["attempts"] == 1
    assert exc_info.value.details["max_attempts"] == 1
    assert exc_info.value.details["retryable"] is False


def test_request_result_encodes_repeated_form_fields() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/dolphinscheduler/projects/7/worker-group"
        assert parse_qs(request.content.decode()) == {
            "workerGroups": ["default", "gpu"]
        }
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": None},
        )

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client:
        payload = client.request_result(
            "POST",
            "projects/7/worker-group",
            form_data={"workerGroups": ["default", "gpu"]},
        )

    assert payload is None


def test_post_payload_can_retry_when_explicitly_marked_retryable() -> None:
    attempts = 0
    profile = ClusterProfile(
        api_url="http://example.test/dolphinscheduler",
        api_token=TEST_API_TOKEN,
        api_retry_attempts=3,
        api_retry_backoff_ms=0,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(
                503,
                json={"message": "temporarily unavailable"},
            )
        return httpx.Response(
            200,
            json={"id": 24, "name": "stream-task"},
        )

    client = DolphinSchedulerClient(
        profile,
        transport=httpx.MockTransport(handler),
    )
    with client:
        payload = client.post_payload(
            "v2/projects/7/task-instances/24",
            json_body={},
            retryable=True,
        )

    assert attempts == 2
    assert payload == {"id": 24, "name": "stream-task"}


def test_get_binary_returns_bytes_and_headers() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert (
            str(request.url)
            == "http://example.test/dolphinscheduler/log/download-log?taskInstanceId=24"
        )
        return httpx.Response(
            200,
            content=b"log-bytes",
            headers={
                "content-type": "text/plain",
                "content-disposition": 'attachment; filename="24.log"',
            },
        )

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client:
        response = client.get_binary("log/download-log", params={"taskInstanceId": 24})

    assert response.content == b"log-bytes"
    assert response.content_type == "text/plain"
    assert response.headers["content-disposition"] == 'attachment; filename="24.log"'


def test_delete_result_unwraps_success_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "DELETE"
        assert (
            str(request.url)
            == "http://example.test/dolphinscheduler/resources?fullName=%2Ftenant%2Fresources%2Fdemo.sql"
        )
        return httpx.Response(
            200,
            json={"code": 0, "msg": "success", "data": None},
        )

    client = DolphinSchedulerClient(
        make_profile(), transport=httpx.MockTransport(handler)
    )
    with client:
        payload = client.delete_result(
            "resources", params={"fullName": "/tenant/resources/demo.sql"}
        )

    assert payload is None

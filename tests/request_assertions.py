"""Assertions shared by prepared-request and installed CLI audit checks."""


def first_dry_run_request(data: object) -> dict[str, object]:
    """Require an ordered request plan and return its first prepared request."""
    assert isinstance(data, dict)
    requests = data.get("requests")
    assert isinstance(requests, list)
    assert requests
    request = requests[0]
    assert isinstance(request, dict)
    assert all(isinstance(key, str) for key in request)
    return dict(request)

from dsctl.config import ClusterProfile

TEST_API_TOKEN = "test-api-token"


def make_profile(
    *,
    api_url: str = "http://example.test/dolphinscheduler",
    api_token: str = TEST_API_TOKEN,
    ds_version: str = "3.4.1",
) -> ClusterProfile:
    return ClusterProfile(
        api_url=api_url,
        api_token=api_token,
        ds_version=ds_version,
    )

import re

from dsctl.config import ClusterProfile

TEST_API_TOKEN = "test-api-token"
ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


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


def normalize_cli_help(text: str) -> str:
    """Return Typer/Rich help text in a stable form for substring assertions."""
    plain = ANSI_ESCAPE_PATTERN.sub("", text)
    return re.sub(r"\s+", " ", plain)

# Security Policy

## Supported Versions

Security fixes target the latest released `0.x` version until the project
adopts a broader support policy.

## Reporting A Vulnerability

Do not open a public issue for secrets, credential exposure, authentication
bypass, or remote-code-execution reports.

Use the repository's private vulnerability reporting channel when it is enabled.
If private reporting is not available yet, contact the project maintainers
directly through the release repository owner.

## Scope

`dsctl` talks to DolphinScheduler REST APIs using credentials provided by the
user. Avoid sharing command output that contains tokens, cluster URLs, or
resource payloads when filing public issues.

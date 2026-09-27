# Legacy API document

`legacy-swagger.json` is a sanitized public Swagger document observed on the
authorized test deployment on 2026-09-09. It contains no credentials or business
records; host, descriptions and examples have been removed.

Its 133 public method/path pairs match the source-visible DS 1.3.9 API after
`@ApiIgnore` exclusions. This does not establish the deployment's exact release:
the observed datasource type enum omits `H2`, which is present in the reviewed
1.3.9 source. Tests must preserve that discrepancy and only admit reads whose
own complete dependency operations have matching observed request facts.

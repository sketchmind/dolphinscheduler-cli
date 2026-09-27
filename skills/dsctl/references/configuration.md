# Connection selection

Use this branch for setup, task isolation, source-precedence failures or an
authorized change to persistent defaults.

1. **Select.** Carry the user's supplied `--context NAME`, `--env-file PATH`,
   `DSCTL_CONTEXT` or `DSCTL_ENV_FILE` into subsequent invocations. Process
   selectors must reach every child process of the task; use absolute env-file
   paths across directories. Continue when the source is unambiguous.
2. **Register when requested.** Use `context create NAME --file FILE` with an
   optional `--project`, then `config set default-context NAME` only when a
   persistent default is intended. Credentials stay in the source file.
   Continue when the saved entry and default match the requested change.
3. **Observe.** Read `context` for the effective URL, name and project. A saved
   default may be shadowed: explicit selectors win over process selectors,
   then any present process `DS_API_URL`, `DS_API_TOKEN` or `DS_VERSION` wins
   over the saved default.
   Complete when this effective target matches the requested task. Run
   `doctor` when remote readiness is also required.

Connection identities never merge. Retry and timeout policies resolve from
process settings before the selected profile and built-in defaults, without
changing the connection. An empty/incomplete process profile still shadows the
saved default; select the intended context explicitly or repair the process
inputs. Version-only `DS_VERSION` supports offline work, not a remote target.
Directory and legacy context files are ignored. A named context supplies only
an optional project default; pass workflow selectors explicitly.

For a saved entry update, use `context update NAME --project PROJECT` or
`--clear-project`. An external file URL change requires an authorized
`context update NAME --file FILE`; this clears the previous project unless
`--project` replaces it. Token rotation at the same URL retains the project.
Inspect `context get NAME` for saved state even if the source file is broken.
Unset `default-context` before deleting its entry. `config` supports only that
key, and all registry operations are local.

## Version selection

For a configured target, an explicit exact version wins over automatic
discovery. Use the deployment's actual release or trusted exact CLI evidence.
Inspect `resolved.target` and version evidence when discovery is uncertain:
API-contract candidates, including a single candidate, are compatible read
evidence rather than the server's exact identity. Execute only an admitted
read; obtain an exact version before writes, exports, templates or lint.

Help, `context` and invocation schema can be inspected without connecting.
Local templates/lint consume an explicit version or fresh cached exact
observation; `doctor` refreshes remote discovery when needed. With no connection
selected, local authoring uses the CLI's offline baseline, which does not
identify a deployment. Preserve the same selected source for discovery,
authoring, preview and apply; an unrelated process `DS_VERSION` does not replace
the version inside an explicitly selected env file or context.

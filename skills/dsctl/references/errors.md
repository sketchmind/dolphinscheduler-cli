# Structured Errors

Use the structured error type and suggestion to ground the next iteration of
the closed loop. For a nonzero exit, read the error envelope on stderr; empty
stdout is not an ambiguous outcome when stderr contains a structured failure.

| Result | Response |
| --- | --- |
| `user_input_error` or usage exit | Follow `error.suggestion`; inspect leaf help or the action schema for any remaining unknown. |
| `not_found` | Refresh scope and select an exact authoritative match. |
| `permission_denied` | Report the required permission as the blocker. |
| `conflict` or `invalid_state` | Refresh live state and ground any necessary, authorized lifecycle mutation. |
| `confirmation_required` | Confirm the risk remains within scope, then retry the same effective input with exactly the returned token. |
| Ambiguous transport result | Inspect `mutation_may_have_applied` and read target state first; retry only when authoritative evidence proves the mutation had no effect. |
| Applied write followed by failure | Preserve `mutation_applied: true` and known identities. Verify the failed readback or remaining stage; a failed command does not cancel the write. |
| `phase: output_render` with `result_available: true` | The operation already returned a result. Inspect preserved `data`, `resolved` and receipts; do not resend a mutation to correct display options. Preview results still do not imply a write. |
| Partial mutation | Inspect `resolved.mutation` or error-detail progress, preserve known identities and completed stages, and resume only missing authorized work. |

An accepted execution can survive an identity-lookup error in
`error.details.execution`. Resolve that receipt through
[runtime guidance](runtime.md) instead of sending another trigger. A generic
transport failure or an empty immediate list alone cannot prove non-application.

Return to the closed loop when the corrected command and its target are fully
grounded. Finish with a blocker when correction requires new authority or an
external state change.

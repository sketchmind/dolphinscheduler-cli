# Structured Errors

Use the structured error type and suggestion to ground the next iteration of
the closed loop.

| Result | Response |
| --- | --- |
| `user_input_error` or usage exit | Follow `error.suggestion`; inspect leaf help or the action schema for any remaining unknown. |
| `not_found` | Refresh scope and select an exact authoritative match. |
| `permission_denied` | Report the required permission as the blocker. |
| `conflict` or `invalid_state` | Refresh live state and ground any necessary, authorized lifecycle mutation. |
| `confirmation_required` | Confirm the risk remains within scope, then retry the same effective input with exactly the returned token. |
| Ambiguous transport result | Read target state first; retry only when that state proves the mutation had no effect. |

Return to the closed loop when the corrected command and its target are fully
grounded. Finish with a blocker when correction requires new authority or an
external state change.

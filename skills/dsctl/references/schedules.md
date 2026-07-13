# Schedule Lifecycle

Use this reference for schedule configuration or lifecycle mutations.

1. **Resolve.** Select the exact schedule with list or get and read its workflow
   binding and current release state. Continue when the schedule id and binding
   are authoritative.
2. **Assess.** Use preview for proposed run times and explain when mutation risk
   or lifecycle constraints affect the decision. Continue when cron, timezone,
   time range, and relevant risk are understood.
3. **Apply.** Treat configuration and activation as separate mutations.
   Create/update changes configuration; execute online only when activation is
   in the requested outcome. After workflow lifecycle changes, refresh schedule
   state. On snapshot conflict, refresh or re-export before rebuilding the
   grounded mutation. Continue when the mutation result is available.
4. **Verify.** Read the schedule back and compare its workflow binding, cron,
   timezone, time range, and release state with the request.

Complete schedule work when the requested fields are authoritative and match
the outcome, or when an authoritative list/get confirms deletion.

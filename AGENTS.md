# AGENTS

## CI

- Any CI surface that boots a Hatchet server instance (engine, API, `hatchet-lite`, docker-compose, or helm) must set `SERVER_SECURITY_CHECK_ENABLED=false`. The check defaults to enabled and phones home to `security.hatchet.run`; CI must never do that. `go test`-based boots are already covered by the test harness; every other boot site sets the var explicitly.

## Migration lineage (fork-specific, 2026-10-31)

- The fork's base cut off upstream at migration `20260831100000_v1_0_149` (2026-08-31 10:00). Two hours later upstream added `20260831120000_v1_0_150` (tenant-scoped concurrency: `v1_tenant_concurrency`, `v1_step_concurrency.tenant_strategy_id`, the slot-insert reactivation trigger). The fork never took that migration; its own wx patches instead used the freed sequence number for `20261015200000_v1_0_150` (internal-tenant deletion guard).
- **Consequence: a database that has ever run fork-built engine code can NOT boot a build that contains the upstream merge** (e.g. the 2026-10-22 merge of upstream main). The migrate tool refuses with `found 8 missing migrations before current version 20261015200000`, because upstream's `_v1_0_150.._v1_0_157` are dated 2026-08-31..09-22 — *before* the fork's applied `20261015200000` — and out-of-order application is rejected by design. Even if that check were bypassed, the merged code would fail at runtime: `ListActiveConcurrencyStrategies` filters on `tenant_strategy_id`, a column those databases do not have.
- **Before cutting an image from a merged base**, apply the pending upstream migrations to the target database first (they are pure DDL against the fork's schema state and do not conflict with the fork's own `_v1_0_150`), then switch the image. Alternatively ship from the pre-merge base (no migration delta at all).
- When adding a fork migration, keep the sequence number strictly above the highest one upstream has used *and* date it after the newest upstream migration, so a later merge cannot invert the order again.

## Code Comments

- Comments should explain non-obvious intent, invariants, or constraints in the current code. Do not mention the old implementation/state (for example, "preserve the behavior of the original query"); state the current rule directly.

## Tests

- Do not include customer names, tenant names, or production namespace names in test names, fixtures, comments, or logs. Describe the scenario by structural properties instead (for example, `dense_high_action_fanout`, not a customer or shard name).

## Docs MDX

- In MDX JSX component bodies, such as `<Callout>`, avoid Markdown link syntax (`[text](href)`). Prettier can wrap the label across lines and break MDX parsing. Use an explicit JSX link instead:

```mdx
<Callout type="info">
  See the{" "}
  <a href="/v1/retry-policies#go-sdk-client-retry-behavior">
    Go SDK client retry behavior section
  </a>
</Callout>
```

## SDK Changes

- Any changes that touch hatchet/sdks/** need to be accompanied by a version bump and changelog entry.

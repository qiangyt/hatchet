# AGENTS

## CI

- Any CI surface that boots a Hatchet server instance (engine, API, `hatchet-lite`, docker-compose, or helm) must set `SERVER_SECURITY_CHECK_ENABLED=false`. The check defaults to enabled and phones home to `security.hatchet.run`; CI must never do that. `go test`-based boots are already covered by the test harness; every other boot site sets the var explicitly.

## Migration lineage (fork-specific, 2026-10-31)

- The fork's base cut off upstream at migration `20260831100000_v1_0_149` (2026-08-31 10:00). Two hours later upstream added `20260831120000_v1_0_150` (tenant-scoped concurrency: `v1_tenant_concurrency`, `v1_step_concurrency.tenant_strategy_id`, the slot-insert reactivation trigger). The fork never took that migration; its own wx patches instead used the freed sequence number for `20261015200000_v1_0_150` (internal-tenant deletion guard).
- A database that has applied the fork's `20261015200000` cannot apply upstream migrations dated before it: the migrate tool rejects out-of-order application. The merged engine also requires the schema created by those migrations, so shipping code and schema separately is not a valid workaround.
- The upstream `_v1_0_150` through `_v1_0_157` files are therefore timestamped `20261015200001` through `20261015200008` in this fork, preserving their relative order. A fork-lineage database applies them automatically, in order, during the same boot that starts the merged engine. Do not move these files earlier or renumber their `_v1_0_*` suffixes.
- When adding a fork migration, keep the sequence number strictly above the highest one upstream has used and date it after the newest migration in this repository, so a later merge cannot invert the order again.

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

- Changes that touch `sdks/**` need to be accompanied by a version bump and a changelog entry for that SDK, with one exception: the Go SDK (`sdks/go/**`). It has no version file or changelog in the repo — it is versioned by git tags on the module path, which are cut after the PR merges. Do not ask for a version bump or changelog entry on Go SDK PRs.

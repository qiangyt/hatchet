# AGENTS

## 设计纪律

**做任何事情，都不能只看最省事的办法，要找最彻底干净的路径。**「省事」不构成方案选型理由：必须先找消除问题根源、不留副本/残留/手工同步点的做法；存在多条路径时，把每条都摆出来并推荐最彻底的那条，而不是只提省事的那条。

- 判断标准：提方案前自问「有没有让问题根源消失、不留第二份真相（副本/镜像/手工同步点）的做法？」——有，它就是首选；「省事」只能是彻底路径的附带优点，永远不能是选它的理由。
- 典型违例：①运行时数据用静态副本替代单一真源（副本必漂移）；②能从被测物本身（镜像/产物/运行实例）取得的数据，却去读源码树或复制文件；③用配置开关/旁路绕开而不是修干净。
- 案例：qc e2e 的 ES 索引模板依赖（2026-10-29）——首选方案提了「复制 code 仓模板进 qc」（省事但引入第二真相），被连续追问两轮后才交出根治方案「从被测 app 镜像内现取模板」（版本一致性构造性成立）。省事方案先出口，即为违规。

## CI

- Any CI surface that boots a Hatchet server instance (engine, API, `hatchet-lite`, docker-compose, or helm) must set `SERVER_SECURITY_CHECK_ENABLED=false`. The check defaults to enabled and phones home to `security.hatchet.run`; CI must never do that. `go test`-based boots are already covered by the test harness; every other boot site sets the var explicitly.

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

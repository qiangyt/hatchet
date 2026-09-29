#!/usr/bin/env python3
#
# Copyright (c) 2026 Yiting Qiang
# SPDX-License-Identifier: MIT
#

"""hatchet fork 的 hatchet-lite 组合镜像构建 + 可选推送（薄编排到 wxinfra.base）。

职责：
1. 前置校验：--upstream-version 必须为三段版本（^v?\\d+\\.\\d+\\.\\d+$，与 wxcount-hatchet SDK
   EngineVersionProbe.VERSION_PATTERN 对齐）——注入非三段版本会让 GetVersion 返回值不可解析
   （2026-09-28 wx.1/wx.2 垃圾戳事故：构建漏传 VERSION、dockerfile 静默缺省 v0.1.0-alpha.0，
   致 SDK 版本 gate 全链故障；servers.dockerfile 已加 fail-fast，本脚本再做入口校验双保险）
2. 跑 frontend/snippets/generate.py（docs 模块生成——lite.dockerfile 前端 build 的隐含前置，
   漏跑则 tsc 报 `@/lib/generated/docs` 缺失）
3. docker build 三中间镜像（servers.dockerfile × lite/admin/migrate，--build-arg VERSION 注入）
4. docker build 组合镜像（lite.dockerfile + HATCHET_{LITE,ADMIN,MIGRATE}_IMAGE 三 ARG）
5. 冒烟断言：组合镜像的 /hatchet/hatchet-lite 二进制含 VERSION 字符串（grep -aqF 静态断言
   版本戳已注入——垃圾戳/空戳在此被拦，不再流出带病镜像）
6. --push：推送组合镜像 + 打印 RepoDigest + 提示 .env 同步清单（推送策略与时机归用户）

tag/VERSION 分离（2026-09-28 拍板）：
- 镜像 tag = v<upstream>-wx.<rev>（标识 fork 构建序号，registry 侧区分多次 fork 构建）
- 注入二进制 VERSION = v<upstream>（标识引擎能力面对齐的上游版本——fork 只加删除/reap
  未改能力面，wxcount-hatchet SDK capability gate 依此判定）

用法（workspace 根 venv）：
  ../venv/bin/python build-image.py                # 裸跑：upstream 取 CHANGELOG.md 顶部版本，fork-rev 缺省 4
  ../venv/bin/python build-image.py --fork-rev 5 --push
  ../venv/bin/python build-image.py --upstream-version v0.105.16 --fork-rev 3   # 显式覆盖（CI/特殊场景）

默认值单一源（防脚本与仓漂移——勿改为硬编码）：
- --upstream-version 缺省 = CHANGELOG.md 顶部 `## [X.Y.Z]`（上游 release 即更新，fork rebase 自动跟随）
- --fork-rev 缺省 = 4（2026-09-29 拍板：固定缺省、人工维护——每次新构建显式 --fork-rev N 递增，
  不做本地镜像史自动推导）

日志：hatchet/logs/build-image.log（console + 文件双写）。退出码：0 成功；非 0 失败。
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# ── 根定位 preamble（workspace 各仓脚本统一形态）─────────────────────
ROOT: Path = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "wxinfra"))

from wxinfra.base.venvcheck import require_workspace_venv  # noqa: E402

require_workspace_venv(ROOT, "hatchet/build-image.py")  # 先于其余 wxinfra import（其依赖 venv 内第三方包）

from wxinfra.base.logging import setup as log_setup  # noqa: E402
from wxinfra.base.process import run  # noqa: E402
from wxinfra.base.proxy import with_proxy  # noqa: E402

REPO_ROOT: Path = Path(__file__).resolve().parent
LOGS_DIR: Path = REPO_ROOT / "logs"

# 与 wxcount-hatchet SDK EngineVersionProbe.VERSION_PATTERN 对齐（可选 v 前缀 + 纯三段数字，
# 拒绝 -alpha/-wx 等任何后缀——fork 序号走 tag 不走 VERSION，见模块 docstring「tag/VERSION 分离」）
_VERSION_PATTERN: re.Pattern[str] = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")

# 三中间镜像（servers.dockerfile 的 SERVER_TARGET 合法取值中组合镜像实际需要的三个）
_SERVER_TARGETS: tuple[str, ...] = ("lite", "admin", "migrate")


def parse_args() -> argparse.Namespace:
    """解析 CLI：--upstream-version / --fork-rev（均有仓内单一源缺省）/ --push / --registry / --goproxy。"""
    parser = argparse.ArgumentParser(
        description="hatchet fork 组合镜像构建（tag=v<upstream>-wx.<rev>，二进制 VERSION=v<upstream>）"
    )
    parser.add_argument(
        "--upstream-version",
        default=None,
        help="引擎能力面对齐的上游版本（如 v0.105.16）——注入二进制、经 GetVersion 外露；"
        "必须纯三段（可选 v 前缀）。缺省 = CHANGELOG.md 顶部 `## [X.Y.Z]`",
    )
    parser.add_argument(
        "--fork-rev",
        default=4,
        type=int,
        help="fork 构建序号（≥1 整数，tag = v<upstream>-wx.<rev>）。缺省 4——固定缺省、人工维护："
        "每次新构建请显式递增（--fork-rev 5、6 ...），不复用既有 tag（2026-09-29 拍板）",
    )
    parser.add_argument(
        "--registry",
        default="gitea2.wxcount.com/docker",
        help="产出组合镜像的 registry 前缀（默认内网 gitea2 docker/ 命名空间）",
    )
    parser.add_argument(
        "--goproxy",
        default="https://goproxy.cn,direct",
        help="Go module proxy（servers.dockerfile GOPROXY build-arg；传空串则不传该 ARG）",
    )
    parser.add_argument("--push", action="store_true", help="构建后推送组合镜像到 registry")
    return parser.parse_args()


def normalize_upstream_version(raw: str) -> str:
    """校验并规范化上游版本为 vX.Y.Z（缺 v 前缀补上）；非法/带后缀 fail-fast。"""
    m: re.Match[str] | None = _VERSION_PATTERN.match(raw)
    if not m:
        raise SystemExit(
            f"✗ --upstream-version 非法：'{raw}'——必须纯三段版本（如 v0.105.16 或 0.105.16），"
            "拒绝 -alpha/-wx 等后缀（fork 序号走 --fork-rev 进 tag，不进 VERSION；"
            "依据：wxcount-hatchet SDK EngineVersionProbe.VERSION_PATTERN）"
        )
    return f"v{m.group(1)}.{m.group(2)}.{m.group(3)}"


# CHANGELOG 顶部 release 标题形态：`## [0.105.16] - 2026-08-31`（上游 Keep-a-Changelog 惯例）
_CHANGELOG_VERSION_PATTERN: re.Pattern[str] = re.compile(r"^## \[(\d+\.\d+\.\d+)\]", re.MULTILINE)


def default_upstream_version() -> str:
    """CHANGELOG.md 顶部第一个 release 版本（fork 对齐的上游单一源）；解析不到 fail-fast。"""
    changelog: Path = REPO_ROOT / "CHANGELOG.md"
    m: re.Match[str] | None = _CHANGELOG_VERSION_PATTERN.search(
        changelog.read_text(encoding="utf-8")
    )
    if not m:
        raise SystemExit(
            f"✗ 无法从 {changelog} 解析上游版本（期望 `## [X.Y.Z]` 标题）——请显式 --upstream-version"
        )
    return normalize_upstream_version(m.group(1))


def docker_build(*, tag: str, dockerfile: str, build_args: list[str]) -> None:
    """单步 docker build（经 with_proxy 注入代理三键；失败经 run(check=True) fail-fast）。"""
    cmd: list[str] = ["docker", "build", "-f", dockerfile, "-t", tag]
    cmd += build_args
    cmd.append(".")
    with_proxy(cmd, proc_name=f"docker-build-{tag}", cwd=REPO_ROOT, timeout=3600)


def main() -> int:
    """编排：docs 生成 → 三中间镜像 → 组合镜像 → 版本戳冒烟 → 可选 push。"""
    args: argparse.Namespace = parse_args()
    version: str = (
        normalize_upstream_version(args.upstream_version)
        if args.upstream_version
        else default_upstream_version()
    )
    fork_rev: int = args.fork_rev
    if fork_rev < 1:
        raise SystemExit(f"✗ --fork-rev 必须 ≥1，got {fork_rev}")
    tag: str = f"{version}-wx.{fork_rev}"
    combo_image: str = f"{args.registry}/hatchet-lite:{tag}"
    goproxy_args: list[str] = ["--build-arg", f"GOPROXY={args.goproxy}"] if args.goproxy else []

    log_setup("build-image", LOGS_DIR)
    print(
        f"[build-image] VERSION={version}"
        f"（{'显式参数' if args.upstream_version else '缺省 ← CHANGELOG.md'}），"
        f"fork-rev={fork_rev}（固定缺省/显式参数——新构建请显式递增）"
        f" → tag={tag}"
    )

    # ── ① docs 生成（lite.dockerfile 前端 build 的隐含前置）──────────────
    print(f"[build-image] ① frontend/snippets/generate.py（docs 模块生成）...")
    run(
        [sys.executable, "generate.py"],
        proc_name="generate-docs",
        cwd=REPO_ROOT / "frontend" / "snippets",
        timeout=120,
    )

    # ── ② 三中间镜像（servers.dockerfile；VERSION 注入在二进制编译期）─────
    for target in _SERVER_TARGETS:
        print(f"[build-image] ② servers.dockerfile SERVER_TARGET={target}（VERSION={version}）...")
        docker_build(
            tag=f"hatchet-{target}-go:{tag}",
            dockerfile="./build/package/servers.dockerfile",
            build_args=[
                "--build-arg",
                f"SERVER_TARGET={target}",
                "--build-arg",
                f"VERSION={version}",
                *goproxy_args,
            ],
        )

    # ── ③ 组合镜像（lite.dockerfile：三二进制 + 前端 + entrypoint）────────
    print(f"[build-image] ③ lite.dockerfile 组合 → {combo_image} ...")
    docker_build(
        tag=combo_image,
        dockerfile="./build/package/lite.dockerfile",
        build_args=[
            "--build-arg",
            f"HATCHET_LITE_IMAGE=hatchet-lite-go:{tag}",
            "--build-arg",
            f"HATCHET_ADMIN_IMAGE=hatchet-admin-go:{tag}",
            "--build-arg",
            f"HATCHET_MIGRATE_IMAGE=hatchet-migrate-go:{tag}",
        ],
    )

    # ── ④ 冒烟断言：二进制含 VERSION 串（垃圾戳/空戳在此拦截）─────────────
    # 组合镜像部署形态为三二进制在根目录（lite.dockerfile COPY 到 ./hatchet-*），非 /hatchet/ 子目录。
    print(f"[build-image] ④ 冒烟断言：/hatchet-lite 含 '{version}' ...")
    with_proxy(
        [
            "docker",
            "run",
            "--rm",
            "--entrypoint",
            "grep",
            combo_image,
            "-aqF",
            version,
            "/hatchet-lite",
        ],
        proc_name="smoke-version-stamp",
        timeout=120,
    )

    print(f"[build-image] ✓ 构建完成：{combo_image}")

    # ── ⑤ 可选推送 ────────────────────────────────────────────────────
    if args.push:
        print(f"[build-image] ⑤ pushing {combo_image} ...")
        with_proxy(["docker", "push", combo_image], proc_name="docker-push", timeout=1800)
        digest: str = with_proxy(
            [
                "docker",
                "inspect",
                "--format",
                "{{index .RepoDigests 0}}",
                combo_image,
            ],
            proc_name="docker-inspect-digest",
            timeout=60,
        ).stdout.strip()
        print(f"[build-image] ✓ pushed：{digest}")
        print(
            "[build-image] .env 同步清单（HATCHET_IMAGE 单一源 code/.env，手工逐字符更新）：\n"
            "  - code/.env\n"
            "  - kailash/deploy/.env\n"
            "  - deploy/.env.e2e（qc e2e 栈共享键）"
        )
    else:
        print("[build-image] 未推送（--push 才推送；推送策略与时机归用户）")
    return 0


if __name__ == "__main__":
    sys.exit(main())

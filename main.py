"""AstrBot information-source aggregator and weekly report reminder."""

from __future__ import annotations

import asyncio
import html
import json
import re
import shlex
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register

REPORT_URL = (
    "https://docs.qq.com/sheet/DWGxaamhOa09rdXJW?tab=pazfci&login_t=1790075799166"
)
SOURCE_TYPES = {"rss", "blog", "github", "github_user"}


@register(
    "astrbot_plugin_week",
    "week",
    "聚合 RSS、博客和 GitHub 信息源，定时推送摘要并提醒填写周报。",
    "1.0.0",
)
class WeekPlugin(Star):
    """Fetch configured sources and send scheduled reports to subscribed sessions."""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._task: asyncio.Task[None] | None = None
        self._state_path = self._find_state_path()
        self._state = self._load_state()
        if bool(config.get("enabled", True)):
            self._task = asyncio.create_task(self._scheduler_loop())

    def _find_state_path(self) -> Path:
        # The plugin lives at <astrbot>/data/plugins/<name>; persist outside it.
        root = Path(__file__).resolve().parents[3]
        path = root / "data" / "plugin_data" / "astrbot_plugin_week" / "state.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _load_state(self) -> dict[str, Any]:
        default = {
            "sources": [],
            "sessions": [],
            "items": [],
            "seen": [],
            "last_run": "",
        }
        try:
            value = json.loads(self._state_path.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                default.update(value)
        except (OSError, ValueError):
            logger.info("week 插件首次运行，创建新的状态文件")
        return default

    def _save_state(self) -> None:
        temp = self._state_path.with_suffix(".tmp")
        temp.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temp.replace(self._state_path)

    def _configured_sources(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        configured = self.config.get("sources", []) or []
        for source in configured:
            if not isinstance(source, dict) or source.get("enabled", True) is False:
                continue
            kind = str(source.get("__template_key", source.get("type", "rss"))).lower()
            if kind not in SOURCE_TYPES or not str(source.get("url", "")).strip():
                continue
            result.append(
                {
                    "name": str(source.get("name") or source["url"]),
                    "type": kind,
                    "url": str(source["url"]).strip(),
                }
            )
        for source in self._state.get("sources", []):
            if (
                isinstance(source, dict)
                and source.get("enabled", True)
                and source.get("type") in SOURCE_TYPES
            ):
                result.append(source)
        unique: dict[tuple[str, str], dict[str, Any]] = {}
        for source in result:
            unique[(source["type"], source["url"])] = source
        return list(unique.values())

    @staticmethod
    def _parse_args(message: str) -> list[str]:
        try:
            return shlex.split(message.strip())
        except ValueError:
            return message.strip().split()

    async def _fetch_text(self, session: aiohttp.ClientSession, url: str) -> str:
        timeout = max(3, int(self.config.get("request_timeout", 20) or 20))
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={"User-Agent": "AstrBot-week/1.0"},
        ) as response:
            response.raise_for_status()
            return await response.text(errors="replace")

    async def _fetch_source(
        self, session: aiohttp.ClientSession, source: dict[str, Any]
    ) -> list[dict[str, str]]:
        kind, url = source["type"], source["url"]
        if kind == "rss":
            return self._parse_feed(
                await self._fetch_text(session, url), source["name"]
            )
        if kind == "github":
            return await self._fetch_github(session, source)
        if kind == "github_user":
            activity = await self._github_user_activity(session, self._github_username(url))
            if not activity:
                return []
            date_key = datetime.now().date().isoformat()
            return [{
                "title": self._format_github_activity(activity),
                "url": f"https://github.com/{activity['login']}?activity={date_key}",
                "source": source["name"],
            }]
        return self._parse_blog(
            await self._fetch_text(session, url), source["name"], url
        )

    @staticmethod
    def _clean(value: str) -> str:
        return re.sub(
            r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", value or ""))
        ).strip()

    def _parse_feed(self, body: str, source_name: str) -> list[dict[str, str]]:
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            return []
        entries = root.findall(".//item") or root.findall(".//{*}entry")
        result = []
        for entry in entries[:20]:
            title = self._clean(
                entry.findtext("title") or entry.findtext("{*}title") or "无标题"
            )
            link_node = entry.find("link")
            if link_node is None:
                link_node = entry.find("{*}link")
            link = (link_node.text or "").strip() if link_node is not None else ""
            if link_node is not None and not link:
                link = str(link_node.attrib.get("href", ""))
            if title and link:
                result.append({"title": title, "url": link, "source": source_name})
        return result

    def _parse_blog(
        self, body: str, source_name: str, base_url: str
    ) -> list[dict[str, str]]:
        title_match = re.search(r"<title[^>]*>(.*?)</title>", body, re.I | re.S)
        page_title = self._clean(title_match.group(1)) if title_match else source_name
        links = re.findall(
            r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', body, re.I | re.S
        )
        result: list[dict[str, str]] = []
        for link, label in links[:20]:
            label = self._clean(label)
            if label and link.startswith(("http://", "https://")):
                result.append({"title": label, "url": link, "source": source_name})
        return result or [{"title": page_title, "url": base_url, "source": source_name}]

    async def _fetch_github(
        self, session: aiohttp.ClientSession, source: dict[str, Any]
    ) -> list[dict[str, str]]:
        parsed = urlparse(source["url"])
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) < 2 or parsed.netloc.lower() != "github.com":
            return []
        api_url = (
            f"https://api.github.com/repos/{parts[0]}/{parts[1]}/releases?per_page=10"
        )
        try:
            body = await self._fetch_text(session, api_url)
            releases = json.loads(body)
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError):
            return []
        if not isinstance(releases, list):
            return []
        return [
            {
                "title": f"{source['name']}: {item.get('name') or item.get('tag_name') or 'release'}",
                "url": item.get("html_url", source["url"]),
                "source": source["name"],
            }
            for item in releases
            if item.get("html_url")
        ]

    @staticmethod
    def _github_username(value: str) -> str:
        parsed = urlparse(value)
        if parsed.netloc.lower() == "github.com":
            return parsed.path.strip("/").split("/")[0]
        return value.strip().strip("/").split("/")[0]

    async def _github_user_activity(
        self, session: aiohttp.ClientSession, username: str, days: int = 30
    ) -> dict[str, Any] | None:
        """Summarize public GitHub events for a user (GitHub retains events for ~90 days)."""
        username = self._github_username(username)
        if not re.fullmatch(r"[A-Za-z0-9-]+", username):
            return None
        try:
            profile = json.loads(await self._fetch_text(session, f"https://api.github.com/users/{username}"))
            events = json.loads(await self._fetch_text(session, f"https://api.github.com/users/{username}/events/public?per_page=100"))
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError):
            return None
        if not isinstance(profile, dict) or not isinstance(events, list):
            return None
        cutoff = datetime.now().astimezone() - timedelta(days=max(1, min(90, days)))
        counts = {"commits": 0, "pull_requests": 0, "issues": 0, "reviews": 0, "comments": 0}
        repositories: set[str] = set()
        latest: str | None = None
        for event in events:
            created = str(event.get("created_at", ""))
            try:
                when = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except ValueError:
                continue
            if when < cutoff:
                continue
            latest = created if latest is None or created > latest else latest
            repo_name = (event.get("repo") or {}).get("name")
            if repo_name:
                repositories.add(str(repo_name))
            event_type = event.get("type")
            payload = event.get("payload") or {}
            if event_type == "PushEvent":
                counts["commits"] += len(payload.get("commits") or [])
            elif event_type == "PullRequestEvent":
                counts["pull_requests"] += 1
            elif event_type == "IssuesEvent":
                counts["issues"] += 1
            elif event_type == "PullRequestReviewEvent":
                counts["reviews"] += 1
            elif event_type in {"IssueCommentEvent", "CommitCommentEvent"}:
                counts["comments"] += 1
        score = counts["commits"] + counts["pull_requests"] * 3 + counts["issues"] * 2 + counts["reviews"] * 2 + counts["comments"]
        return {
            "login": profile.get("login", username),
            "name": profile.get("name") or profile.get("login", username),
            "public_repos": profile.get("public_repos", 0),
            "followers": profile.get("followers", 0),
            "counts": counts,
            "repositories": sorted(repositories),
            "latest": latest,
            "days": max(1, min(90, days)),
            "score": score,
        }

    @staticmethod
    def _format_github_activity(activity: dict[str, Any]) -> str:
        counts = activity["counts"]
        return (
            f"GitHub 用户 {activity['name']}（{activity['login']}）近 {activity['days']} 天："
            f"活跃度 {activity['score']}，提交 {counts['commits']}，PR {counts['pull_requests']}，"
            f"Issue {counts['issues']}，Review {counts['reviews']}，评论 {counts['comments']}，"
            f"活跃仓库 {len(activity['repositories'])} 个"
        )

    async def _collect_new_items(self) -> list[dict[str, str]]:
        found: list[dict[str, str]] = []
        async with aiohttp.ClientSession() as session:
            for source in self._configured_sources():
                try:
                    found.extend(await self._fetch_source(session, source))
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                    logger.warning("抓取信息源失败 %s: %s", source.get("url"), exc)
        seen = set(self._state.get("seen", []))
        new_items = [item for item in found if item["url"] not in seen]
        self._state["seen"] = list(
            dict.fromkeys(
                [*self._state.get("seen", []), *(item["url"] for item in found)]
            )
        )[-500:]
        self._state["items"] = [*self._state.get("items", []), *new_items][-200:]
        return new_items

    def _format_report(
        self, items: list[dict[str, str]], scheduled: bool = False
    ) -> str:
        max_items = max(1, int(self.config.get("max_items", 10) or 10))
        lines = ["本期信息简报" if scheduled else "信息源抓取结果", ""]
        if items:
            for item in items[:max_items]:
                lines.append(
                    f"- {item.get('title', '无标题')}\n  {item.get('url', '')}"
                )
        else:
            lines.append("本次没有发现新的信息。")
        lines.extend(["", f"请填写周报：{self.config.get('report_url') or REPORT_URL}"])
        return "\n".join(lines)

    async def _run_cycle(self, extra_targets: list[str] | None = None) -> tuple[int, int]:
        items = await self._collect_new_items()
        self._state["last_run"] = datetime.now().isoformat(timespec="seconds")
        self._save_state()
        manual_items = [
            item
            for item in self._state.get("items", [])
            if item.get("source") == "自定义"
        ][-3:]
        message = self._format_report([*manual_items, *items], scheduled=True)
        sent = 0
        targets = list(dict.fromkeys([*self._state.get("sessions", []), *(extra_targets or [])]))
        for target in targets:
            try:
                if await self.context.send_message(
                    target, MessageChain().message(message)
                ):
                    sent += 1
            except (
                Exception
            ) as exc:  # One invalid session must not stop other recipients.
                logger.warning("发送周报提醒失败 %s: %s", target, exc)
        return len(items), sent

    async def _scheduler_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._seconds_until_next_run())
                await self._run_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("week 定时任务异常: %s", exc)
                await asyncio.sleep(60)

    def _seconds_until_next_run(self) -> float:
        period = str(self.config.get("period", "weekly"))
        if period == "interval":
            return max(3600, int(self.config.get("interval_hours", 168) or 168) * 3600)
        now = datetime.now()
        hour, minute = 9, 0
        match = re.fullmatch(
            r"(\d{1,2}):(\d{2})", str(self.config.get("time", "09:00"))
        )
        if match:
            hour, minute = min(23, int(match.group(1))), min(59, int(match.group(2)))
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if period == "weekly":
            weekday = min(7, max(1, int(self.config.get("weekday", 1) or 1))) - 1
            target += timedelta(days=(weekday - target.weekday()) % 7)
            if target <= now:
                target += timedelta(days=7)
        else:
            if target <= now:
                target += timedelta(days=1)
        return max(30.0, (target - now).total_seconds())

    @filter.command("week_help")
    async def week_help(self, event: AstrMessageEvent):
        yield event.plain_result(
            "/week_subscribe 订阅提醒\n/week_unsubscribe 取消订阅\n/week_sources 查看来源\n/week_add_source 名称 rss|blog|github|github_user URL\n/week_github_user 用户名 [天数]\n/week_del_source 名称\n/week_add_item 标题 URL [备注]\n/week_items 查看自定义条目\n/week_fetch 立即抓取\n/week_publish 立即发布测试\n/week_report 查看当前周报\n/week_help 查看帮助"
        )

    @filter.command("week_subscribe")
    async def week_subscribe(self, event: AstrMessageEvent):
        target = event.unified_msg_origin
        if target not in self._state["sessions"]:
            self._state["sessions"].append(target)
            self._save_state()
        yield event.plain_result("已订阅定时信息简报和周报提醒。")

    @filter.command("week_unsubscribe")
    async def week_unsubscribe(self, event: AstrMessageEvent):
        self._state["sessions"] = [
            item for item in self._state["sessions"] if item != event.unified_msg_origin
        ]
        self._save_state()
        yield event.plain_result("已取消订阅。")

    @filter.command("week_sources")
    async def week_sources(self, event: AstrMessageEvent):
        sources = self._configured_sources()
        text = (
            "\n".join(
                f"- {item['name']} [{item['type']}] {item['url']}" for item in sources
            )
            or "暂无信息源。"
        )
        yield event.plain_result(text)

    @filter.command("week_add_source")
    async def week_add_source(self, event: AstrMessageEvent):
        args = self._parse_args(event.message_str)[1:]
        if len(args) < 3 or args[1].lower() not in SOURCE_TYPES:
            yield event.plain_result("用法：/week_add_source 名称 rss|blog|github URL")
            return
        source = {
            "name": args[0],
            "type": args[1].lower(),
            "url": args[2],
            "enabled": True,
        }
        self._state["sources"] = [
            item
            for item in self._state["sources"]
            if item.get("name") != source["name"]
        ]
        self._state["sources"].append(source)
        self._save_state()
        yield event.plain_result(f"已添加信息源：{source['name']}")

    @filter.command("week_del_source")
    async def week_del_source(self, event: AstrMessageEvent):
        args = self._parse_args(event.message_str)[1:]
        if not args:
            yield event.plain_result("用法：/week_del_source 名称")
            return
        before = len(self._state["sources"])
        self._state["sources"] = [
            item for item in self._state["sources"] if item.get("name") != args[0]
        ]
        self._save_state()
        yield event.plain_result(
            "已删除。"
            if len(self._state["sources"]) < before
            else "未找到该动态信息源。"
        )

    @filter.command("week_add_item")
    async def week_add_item(self, event: AstrMessageEvent):
        args = self._parse_args(event.message_str)[1:]
        if len(args) < 2:
            yield event.plain_result("用法：/week_add_item 标题 URL [备注]")
            return
        item = {
            "title": args[0],
            "url": args[1],
            "source": "自定义",
            "note": " ".join(args[2:]),
        }
        self._state["items"] = [*self._state.get("items", []), item][-200:]
        self._save_state()
        yield event.plain_result("已添加自定义条目。")

    @filter.command("week_items")
    async def week_items(self, event: AstrMessageEvent):
        items = self._state.get("items", [])[-10:]
        yield event.plain_result(
            "\n".join(f"- {item.get('title')}\n  {item.get('url')}" for item in items)
            or "暂无条目。"
        )

    @filter.command("week_fetch")
    async def week_fetch(self, event: AstrMessageEvent):
        items = await self._collect_new_items()
        self._save_state()
        yield event.plain_result(self._format_report(items))

    @filter.command("week_publish", alias={"week_publish_now", "week_test"})
    async def week_publish(self, event: AstrMessageEvent):
        """立即抓取并发布一次，用于测试定时推送链路。"""
        item_count, sent_count = await self._run_cycle(
            extra_targets=[event.unified_msg_origin]
        )
        yield event.plain_result(
            f"立即发布测试完成：发现 {item_count} 条新信息，已发送到 {sent_count} 个会话。"
        )

    @filter.command("week_github_user")
    async def week_github_user(self, event: AstrMessageEvent):
        """查询 GitHub 用户在最近一段时间内的公开活跃度。"""
        args = self._parse_args(event.message_str)[1:]
        if not args:
            yield event.plain_result("用法：/week_github_user 用户名 [天数]\n例如：/week_github_user torvalds 30")
            return
        try:
            days = int(args[1]) if len(args) > 1 else 30
        except ValueError:
            days = 30
        async with aiohttp.ClientSession() as session:
            activity = await self._github_user_activity(session, args[0], days)
        if not activity:
            yield event.plain_result("无法获取该 GitHub 用户。请确认用户名正确，或稍后重试（GitHub API 可能触发限流）。")
            return
        counts = activity["counts"]
        repos = "、".join(activity["repositories"][:8]) or "无"
        latest = activity["latest"] or "暂无"
        yield event.plain_result(
            f"GitHub 用户活跃度：{activity['name']} (@{activity['login']})\n"
            f"统计窗口：最近 {activity['days']} 天\n"
            f"活跃度分数：{activity['score']}\n"
            f"提交：{counts['commits']}，PR：{counts['pull_requests']}，Issue：{counts['issues']}\n"
            f"Review：{counts['reviews']}，评论：{counts['comments']}\n"
            f"活跃仓库：{len(activity['repositories'])} 个（{repos}）\n"
            f"公开仓库：{activity['public_repos']}，Followers：{activity['followers']}\n"
            f"最近活动：{latest}\n"
            f"主页：https://github.com/{activity['login']}"
        )

    @filter.command("week_report")
    async def week_report(self, event: AstrMessageEvent):
        items = self._state.get("items", [])[
            -max(1, int(self.config.get("max_items", 10) or 10)) :
        ]
        yield event.plain_result(self._format_report(items, scheduled=True))

    async def terminate(self):
        if self._task and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._save_state()

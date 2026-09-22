# 信息源与周报提醒

AstrBot 插件，用于聚合 RSS/Atom、普通博客网页和 GitHub 仓库更新，并按配置的时间推送到订阅会话。

## 使用

首次使用先在希望接收消息的会话发送 `/week_subscribe`。插件配置可在 AstrBot WebUI 中填写默认来源和调度参数；也可以使用命令动态维护：

```text
/week_sources
/week_add_source 名称 rss https://example.com/feed.xml
/week_add_source 名称 blog https://example.com/blog
/week_add_source 名称 github https://github.com/astral-sh/ruff
/week_del_source 名称
/week_add_item 标题 https://example.com/article 备注
/week_fetch
/week_report
/week_help
```

动态来源、订阅会话和自定义条目保存在 AstrBot 根目录的 `data/plugin_data/astrbot_plugin_week/state.json`，不会因插件更新覆盖。

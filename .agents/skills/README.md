# TuAlpha Agent Skills

本目录保存项目随代码维护的 Agent Skills。

## `tualpha-strategy`

用于让 Agent 根据 TuAlpha 0.5 的真实 API、事件时序、中国市场规则和数据字段编写或审查策略。

```text
/skill:tualpha-strategy
```

带参数调用示例：

```text
/skill:tualpha-strategy 为 510300.SH 编写 20/60 日均线策略，2020-2025 回测
```

目录：

```text
tualpha-strategy/
├── SKILL.md
├── references/
│   ├── framework-contract.md
│   ├── api-reference.md
│   └── data-fields.md
└── assets/
    ├── single-asset-template.py
    └── multi-asset-template.py
```

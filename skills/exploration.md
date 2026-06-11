# Skills Folder Exploration Report

**Date:** 2026-06-11
**Scope:** Every file and folder under `skills/` in `twitter-crawler`, read before any refactoring work began.

---

## 1. Executive Summary

The `skills/` folder does **not** contain Twitter-crawler-specific conventions. It contains
three **generic, Anthropic-authored Claude Code skill packs** (`claude-api`, `mcp-builder`,
`skill-creator`), each Apache-2.0 licensed and oriented at helping Claude perform
*other* tasks (building Claude API apps, building MCP servers, authoring skills).

None of them define naming rules, base classes, or config patterns for this scraper.
The actual architectural source of truth for the refactor is the **reference TikTok
project** (`D:\Kerjaan\project\portofolio\tiktok-end-to-end-crawler`) named in the task
brief (Step 2). The skills do, however, encode general engineering values that are
consistent with — and reinforce — the refactoring brief (see §4).

---

## 2. Inventory — every file and folder

### 2.1 `skills/claude-api/` — "Build, debug, and optimize Claude API / Anthropic SDK apps"

| Path | Contents |
|---|---|
| `SKILL.md` | Entry point. Language detection, model catalog (Opus 4.7 default), thinking/effort parameters, prompt caching quick reference, Managed Agents overview, reading guide, common pitfalls. |
| `LICENSE.txt` | Apache License 2.0. |
| `csharp/claude-api.md` | C# SDK usage (Messages API, no tool runner). |
| `curl/examples.md` | Raw HTTP examples for the Messages API. |
| `curl/managed-agents.md` | Raw HTTP examples for Managed Agents endpoints. |
| `go/claude-api.md` | Go SDK usage, `BetaToolRunner`. |
| `go/managed-agents/README.md` | Managed Agents flows in Go. |
| `java/claude-api.md` | Java SDK usage, annotated-class tool use. |
| `java/managed-agents/README.md` | Managed Agents flows in Java. |
| `php/claude-api.md` | PHP SDK usage, `toolRunner()`, structured output models. |
| `php/managed-agents/README.md` | Managed Agents flows in PHP. |
| `python/claude-api/README.md` | Python SDK install, quick start, common patterns, error handling. |
| `python/claude-api/batches.md` | Batches API (async, 50% cost) in Python. |
| `python/claude-api/files-api.md` | Files API (upload once, reference by `file_id`) in Python. |
| `python/claude-api/streaming.md` | Streaming responses in Python. |
| `python/claude-api/tool-use.md` | Tool runner + manual tool loop in Python. |
| `python/managed-agents/README.md` | Managed Agents flows in Python. |
| `ruby/claude-api.md` | Ruby SDK usage, beta `tool_runner`. |
| `ruby/managed-agents/README.md` | Managed Agents flows in Ruby. |
| `shared/agent-design.md` | Agent design heuristics: tool surface, context management, caching strategy. |
| `shared/error-codes.md` | HTTP error code reference and handling guidance. |
| `shared/live-sources.md` | WebFetch URLs for live official documentation. |
| `shared/managed-agents-api-reference.md` | Endpoint reference (agents, sessions, environments, vaults, memory stores). |
| `shared/managed-agents-client-patterns.md` | Client-side patterns: stream reconnect, interrupt, tool confirmation. |
| `shared/managed-agents-core.md` | Core concepts: agent, session, environment, container. |
| `shared/managed-agents-environments.md` | Environments as reusable container config templates. |
| `shared/managed-agents-events.md` | SSE event stream and steering. |
| `shared/managed-agents-memory.md` | Memory stores (public beta). |
| `shared/managed-agents-multiagent.md` | Coordinator/sub-agent threads sharing one container. |
| `shared/managed-agents-onboarding.md` | Guided interview flow for setting up a Managed Agent. |
| `shared/managed-agents-outcomes.md` | Outcome-driven iterate/grade/revise loops. |
| `shared/managed-agents-overview.md` | Managed Agents surface overview and reading guide. |
| `shared/managed-agents-self-hosted-sandboxes.md` | Self-hosted tool execution containers. |
| `shared/managed-agents-tools.md` | Server vs client tools, Skills integration. |
| `shared/managed-agents-webhooks.md` | Webhook notifications for resource state changes. |
| `shared/model-migration.md` | Migrating between Claude model versions; breaking changes. |
| `shared/models.md` | Exact model ID catalog. |
| `shared/prompt-caching.md` | Prefix-stability design, breakpoint placement, silent invalidators. |
| `shared/tool-use-concepts.md` | Conceptual foundations of tool use. |
| `typescript/claude-api/*.md` (README, batches, files-api, streaming, tool-use) | TypeScript equivalents of the Python docs. |
| `typescript/managed-agents/README.md` | Managed Agents flows in TypeScript. |

**Verdict:** Pure Claude-API developer documentation. Irrelevant to scraping Twitter;
relevant only if this project ever adds an LLM-powered feature.

### 2.2 `skills/mcp-builder/` — "Create high-quality MCP servers"

| Path | Contents |
|---|---|
| `SKILL.md` | Four-phase workflow: research → implementation → review/test → evaluation. |
| `LICENSE.txt` | Apache License 2.0. |
| `reference/mcp_best_practices.md` | Naming conventions, response formats, pagination, transport selection, security/error standards. |
| `reference/python_mcp_server.md` | Python/FastMCP guide: Pydantic input models, `@mcp.tool` registration, quality checklist. |
| `reference/node_mcp_server.md` | TypeScript guide: Zod schemas, `server.registerTool`, project structure. |
| `reference/evaluation.md` | How to create evaluation question sets for MCP servers. |
| `scripts/connections.py` | ABC-based connection-handling helpers for MCP transports (stdio/HTTP/SSE). |
| `scripts/evaluation.py` | Evaluation harness driving Claude against an MCP server. |
| `scripts/example_evaluation.xml` | Example QA-pair evaluation file. |
| `scripts/requirements.txt` | `anthropic>=0.39.0`, `mcp>=1.1.0`. |

**Verdict:** About building MCP servers, not crawlers. But its Python guidance
(Pydantic validation, async I/O, actionable errors, DRY, full type coverage)
matches the refactoring brief's quality bar.

### 2.3 `skills/skill-creator/` — "Create and iteratively improve skills"

| Path | Contents |
|---|---|
| `SKILL.md` | Skill authoring loop: draft → test → review → improve; description optimization; packaging. |
| `LICENSE.txt` | Apache License 2.0. |
| `agents/analyzer.md` | Sub-agent prompt: analyze benchmark results. |
| `agents/comparator.md` | Sub-agent prompt: blind A/B output comparison. |
| `agents/grader.md` | Sub-agent prompt: grade assertions against transcripts. |
| `assets/eval_review.html` | HTML template for reviewing trigger-eval query sets. |
| `eval-viewer/generate_review.py` | Generates/serves the eval-results review page. |
| `eval-viewer/viewer.html` | The review page template. |
| `references/schemas.md` | JSON schemas for `evals.json`, `grading.json`, `benchmark.json`. |
| `scripts/__init__.py` | Empty package marker. |
| `scripts/aggregate_benchmark.py` | Aggregates grading results into benchmark stats. |
| `scripts/generate_report.py` | HTML report from description-optimization runs. |
| `scripts/improve_description.py` | Improves a skill description via `claude -p`. |
| `scripts/package_skill.py` | Packages a skill folder into a `.skill` file. |
| `scripts/quick_validate.py` | Minimal skill validation. |
| `scripts/run_eval.py` | Trigger-evaluation runner. |
| `scripts/run_loop.py` | Eval+improve loop with train/test split. |
| `scripts/utils.py` | Shared path utilities. |

**Verdict:** Tooling for authoring Claude skills. No bearing on crawler architecture.

---

## 3. Key patterns, conventions, and constraints discovered

1. **Progressive disclosure / layered documentation** — every skill keeps a small
   entry point (`SKILL.md`) and pushes detail into `reference/`/`shared/` files read
   on demand. Mirrored in the refactor by a small `CLAUDE.md` + focused modules.
2. **Explain the why, not heavy-handed MUSTs** (skill-creator) — applies to docstrings
   and CLAUDE.md tone.
3. **Schema-first contracts** — mcp-builder mandates Pydantic (Python) / Zod (TS) for
   every input; outputs get explicit schemas. Directly maps to the brief's
   "Pydantic v2 `BaseModel` for all data contracts."
4. **Async-first I/O** — mcp-builder: "Async/await for I/O operations." Matches the
   brief's asyncio requirement.
5. **Actionable error messages** and consistent error handling — maps to a dedicated
   exception layer.
6. **No secrets in source; keep things lean ("remove what isn't pulling its weight")**
   — matches the brief's git-hygiene and no-dead-code constraints.
7. **Licensing constraint:** all three skill packs are Apache-2.0; the project root
   carries its own `LICENSE` (MIT-style, 1 KB). The skill packs are vendored tooling,
   not project source — they should be excluded from the published package or kept
   clearly separated.

## 4. Architecture decisions implied for the refactor

- The skills impose **no** crawler architecture. Architecture comes from the
  reference TikTok project: `source/` root with `controllers/` (crawler types behind
  a registry), `helpers/input` + `helpers/output` driver factories (std, file, kafka,
  elasticsearch), `library/` (config, schemas, API client, infra setup), `exception/`,
  `tests/`, `deployment/` (compose + k8s), plus root `config.yaml`, `Dockerfile`.
- For this project, `controller/tiktok/` → `controller/twitter/` (per Step 2), with
  everything else mirrored and adapted to Twitter/Nitter semantics.
- The generic skill guidance reinforces: Pydantic v2 everywhere, `pydantic-settings`
  for config, async `httpx`/`aiokafka`/ES, factory + Open/Closed extension points,
  typed exceptions, pytest + pytest-asyncio.

## 5. Direct influences on the Twitter scraper refactor

- **Output drivers as a factory** (from the reference project, validated by the
  skills' DRY/ABC patterns): Telegram alerting and JSON-file snapshots from
  `twitter.py` become pluggable output/notification concerns, not inline calls.
- **Controllers register themselves** so a new crawler type (e.g. `user_tweets`)
  is added by dropping in a module — core pipeline untouched (OCP).
- **All tunables** (mirrors, query, user agent, retry counts, Telegram token/chat id,
  Kafka topic, ES index) move to `BaseSettings`-backed env vars.
- **Naming:** follow the reference project's snake_case modules and
  `controllers/<platform>/<action>.py` layout.

## 6. Open questions / ambiguities

1. **Why is `skills/` here at all?** It looks like vendored Claude Code skill packs
   (perhaps copied for offline use). Assumed intent: AI-assisted development tooling,
   not runtime code. It is left untouched by the refactor.
2. **Brief says `controller/twitter/`, reference project uses `controllers/` (plural).**
   Resolved in favor of mirroring the reference project exactly: `controllers/twitter/`.
3. **Should `skills/` ship in the public repo?** It adds ~70 files of third-party
   Apache-2.0 content to a portfolio scraper. Left in place (not my call to delete),
   but flagged for the owner.
4. **`twitter.py` hardcodes a query that overrides `--query`** (`main()` reassigns
   `query` after parsing args) — a live bug the refactor removes.
5. **Telegram messages are in Indonesian** — kept as configurable text, with defaults
   translated to English for the public repo? Chosen: keep messages simple English;
   content is config/notifier-level, easy to adjust.

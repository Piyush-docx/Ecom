# AGENTS.md

Instructions for any IDE coding agent (Claude Code, Cursor, Copilot,
Codex, Windsurf, Antigravity, Gemini CLI, etc.) working in this repo.

> Source of truth for **what** to build: `IMPLEMENTATION_PLAN.md`. This
> file governs **how** to build it — which workflow skill to invoke at
> each phase, and the non-negotiable rules specific to this project.

## 1. Install the skills once, before Phase 0

This project uses [addyosmani/agent-skills](https://github.com/addyosmani/agent-skills),
a library of senior-engineer workflow skills (spec-writing, task
breakdown, TDD, code review, security review, etc.), not files copied by
hand into this repo.

**Fastest path — works across 70+ agents**, via the [skills CLI](https://github.com/vercel-labs/skills):

```bash
# install just the skills this project actually needs
npx skills add addyosmani/agent-skills --skill using-agent-skills
npx skills add addyosmani/agent-skills --skill spec-driven-development
npx skills add addyosmani/agent-skills --skill planning-and-task-breakdown
npx skills add addyosmani/agent-skills --skill context-engineering
npx skills add addyosmani/agent-skills --skill source-driven-development
npx skills add addyosmani/agent-skills --skill incremental-implementation
npx skills add addyosmani/agent-skills --skill doubt-driven-development
npx skills add addyosmani/agent-skills --skill api-and-interface-design
npx skills add addyosmani/agent-skills --skill test-driven-development
npx skills add addyosmani/agent-skills --skill debugging-and-error-recovery
npx skills add addyosmani/agent-skills --skill code-review-and-quality
npx skills add addyosmani/agent-skills --skill code-simplification
npx skills add addyosmani/agent-skills --skill security-and-hardening
npx skills add addyosmani/agent-skills --skill performance-optimization
npx skills add addyosmani/agent-skills --skill observability-and-instrumentation
npx skills add addyosmani/agent-skills --skill git-workflow-and-versioning
npx skills add addyosmani/agent-skills --skill ci-cd-and-automation
npx skills add addyosmani/agent-skills --skill documentation-and-adrs
npx skills add addyosmani/agent-skills --skill shipping-and-launch
```

Or, simpler, just install everything and ignore what doesn't apply:

```bash
npx skills add addyosmani/agent-skills
```

**Deliberately excluded** (not relevant to a backend-only project):
`interview-me`, `idea-refine` (the idea is already scoped — this file and
`IMPLEMENTATION_PLAN.md` are the output of that step), `frontend-ui-engineering`,
`browser-testing-with-devtools` (no browser UI in this project).

**Claude Code users** can instead do a full plugin install:
```bash
/plugin marketplace add addyosmani/agent-skills
/plugin install agent-skills@addy-agent-skills
```

**Cursor users**: sync `skills/` from a local clone into `.cursor/skills/`
per [docs/cursor-setup.md](https://github.com/addyosmani/agent-skills/blob/main/docs/cursor-setup.md)
in that repo.

## 2. Non-negotiable rules for this repo

These apply regardless of which skill is currently active — carried over
from `using-agent-skills`' core operating behaviors, sharpened for this
project's specific failure modes:

1. **Surface assumptions before any non-trivial decision.** §1 of
   `IMPLEMENTATION_PLAN.md` lists the defaults already chosen. If you're
   about to deviate from one of them, say so explicitly and wait for
   confirmation — don't quietly swap Kafka for RabbitMQ or Go for Node
   mid-build.
2. **The rate limiter is never allowed to be a race condition.** Any check
   against Redis that isn't a single atomic Lua script is wrong, full
   stop — even if it "probably works" under light load. See Phase 2 and
   §5 of the plan.
3. **Every Kafka consumer is idempotent.** At-least-once delivery is the
   default; a consumer that assumes exactly-once will double-charge a
   customer eventually. See Phase 5.
4. **No phase starts until the previous phase's acceptance criteria in
   `IMPLEMENTATION_PLAN.md` actually pass** — not "looks right," an actual
   passing test or a captured load-test number.
5. **Scope discipline.** Touch only the module for the current phase.
   Don't refactor the gateway while implementing the catalog service
   because you noticed something.
6. **Load-test numbers are measured, never estimated.** If a resume bullet
   needs a throughput or latency figure, it comes from a saved k6 run in
   `deploy/k6/results/`, not a plausible-sounding guess.

## 3. Phase → skill mapping

Work through `IMPLEMENTATION_PLAN.md` §4 in order. At each phase, invoke
the matching skill(s) before writing code for that phase:

| Plan phase | Primary skill(s) to invoke | Also pull in when... |
|---|---|---|
| 0 — Spec lock & scaffold | `spec-driven-development` | — |
| 1 — Rate limiter core | `planning-and-task-breakdown` → `incremental-implementation` → `test-driven-development` | `doubt-driven-development` on the sliding-window boundary logic — it's the easiest place to get subtly wrong |
| 2 — Redis-backed, atomic | `incremental-implementation` → `test-driven-development` | `source-driven-development` (verify Redis Lua/EVAL semantics against official docs, don't guess); `doubt-driven-development` on the atomicity guarantee itself before calling it done |
| 3 — Gateway + middleware | `api-and-interface-design` → `incremental-implementation` | `security-and-hardening` for the JWT validation path |
| 4 — Microservices CRUD | `api-and-interface-design` → `incremental-implementation` → `test-driven-development` | `context-engineering` if the agent's context is getting crowded working across 4 services — load one service at a time |
| 5 — Saga | `doubt-driven-development` first (this is the highest-stakes design decision in the project) → `incremental-implementation` → `test-driven-development` | — |
| 6 — Observability | `observability-and-instrumentation` | — |
| 7 — Integration + load test | `performance-optimization` | `debugging-and-error-recovery` when something in the full stack doesn't behave like its isolated test suggested |
| 8 — Documentation | `documentation-and-adrs` | — |
| 9 — Ship | `code-review-and-quality` → `code-simplification` → `git-workflow-and-versioning` → `ci-cd-and-automation` → `shipping-and-launch` | `security-and-hardening` as a final pass |

If you installed the Claude Code plugin, the equivalent slash-command
flow per phase is `/spec` → `/plan` → `/build` → `/test` → `/review` →
`/webperf` (maps to `performance-optimization`, use it for Phase 7) →
`/ship`.

## 4. Starting a session

1. Read `IMPLEMENTATION_PLAN.md` in full if you haven't this session.
2. Check which phase's acceptance criteria are not yet satisfied — that's
   the current phase. Don't assume; check for passing tests / existing
   code to confirm where the project actually is, not where the last
   commit message claims it is.
3. Invoke the skill(s) from §3 for that phase.
4. Before implementing anything non-trivial in that phase, state your
   assumptions per rule §2.1 and wait if anything is ambiguous.
5. Do not move to the next phase until the current one's acceptance
   criteria pass with actual evidence (test output, captured numbers).

## 5. Definition of done

Once skills are installed, `references/definition-of-done.md` in the
`agent-skills` package is the project-wide bar for every change,
regardless of phase: tests pass, no regressions, behavior verified at
runtime, docs updated. Each phase's acceptance criteria in
`IMPLEMENTATION_PLAN.md` are the *local* check; the definition of done is
the *global* one. Both must hold.
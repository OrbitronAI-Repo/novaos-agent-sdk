# `migrate` CLI

`python -m novaos_client_sdk.migrate` ports an agent built against the pre-migration ACP shell onto the A2A shell, in three stages: `analyze` (read-only, classifies the agent and writes a plan), `transform` (rewrites the agent per that plan), `verify` (runs the migrated agent for real and checks it). See [Migrating from 1.x](../guides/migrating-from-1x.md) for the end-to-end walkthrough; this page is the flag/exit-code/output-file reference for each subcommand.

`transform` needs the `migrate` extra, for `libcst`:

```bash
pip install "novaos-agent-sdk[migrate]"
```

`analyze` and `verify` need no extra — `analyze` is a pure `ast` walk, and `verify` calls the migrated agent over this SDK's own `Client`, both already core dependencies.

## `analyze`

Classifies an agent's `/runs` shape and every ACP-shaped surface in its source, without ever importing or executing the agent — everything comes from a static `ast` walk — and never writes inside the tree it analyzes.

```bash
python -m novaos_client_sdk.migrate analyze <path> [--out DIR] [--fleet]
```

| Flag | Default | Meaning |
|---|---|---|
| `path` | — | Agent directory, or a fleet root with `--fleet` |
| `--out` | none | Directory to write `<agent>.plan.json` / `<agent>.plan.md` into. Without it, `analyze` still runs and prints its result but writes nothing to disk |
| `--fleet` | off | Treat `path` as a directory of agent directories, analyzing each. Directories with no `manifest.json` are skipped; `commons`, `sdk`, and `test_kit` are always skipped as non-agent siblings |

**Output:** single-agent mode prints the plan as JSON on stdout and, with `--out`, writes `<agent>.plan.json` / `<agent>.plan.md`. Fleet mode prints a Markdown summary table and, with `--out`, additionally writes `fleet-summary.md` and `fleet-summary.json` (all plans plus `skipped`/`errors` lists).

**Exit codes** (single-agent and fleet alike):

| Code | Meaning |
|---|---|
| `0` | No blockers found |
| `1` | At least one blocker found |
| `2` | Could not analyze — a target had no `manifest.json`, or a `.py` file in it failed to parse |

### `plan.json` fields at a glance

| Field | Meaning |
|---|---|
| `schema_version` | Version of this JSON shape (bump-on-break, not on every field) |
| `agent`, `agent_path` | Agent name and resolved source path |
| `manifest` | Facts read from `manifest.json`: `schema_version`, `protocol`, `skills`, `sdk_path`, `vendored_sdk_version`, `exposed_port` |
| `shape`, `shape_reason` | Which ACP `/runs` shape (`A`, `B`, `C`, or `unknown`) and why |
| `mixed` | Whether the agent has both a streaming and a non-streaming branch |
| `surfaces` | Every ACP-shaped construct found, each `{kind, file, line, detail, extra}` |
| `blockers` | Surfaces that are reachable and will break post-migration, each `{kind, file, line, value, effect}` |
| `dead_code` | ACP-tainted files nothing reachable imports — safe for the codemod to delete, each `{file, reason}` |
| `tests` | Test-file inventory: `total_files`, `runs_route_files`, `make_ctx_files`, `routing_inference_files`, `fixture_files` |
| `codemod` | `{eligible, actions, refusal_reason}` — whether `transform` can run unattended, and on what |
| `manual` | Notes on rewrites the codemod cannot make mechanically |
| `verify_fixtures` | Which `tests/test-data/*.json` files `verify` should convert into live requests |

## `transform`

Applies every action named in `plan.codemod.actions` to the agent tree — mechanical rewrites only, never guessed heuristics. An action string this SDK version has no handler for, or a plan whose `codemod.eligible` is `False`, is refused with the plan's own `refusal_reason` rather than attempted.

```bash
python -m novaos_client_sdk.migrate transform <path> --plan PLAN.json [--write]
```

| Flag | Default | Meaning |
|---|---|---|
| `path` | — | Agent directory to rewrite |
| `--plan` | required | Path to the agent's `<agent>.plan.json` from `analyze` |
| `--write` | off | Apply the rewrite. Without it, only a unified diff is printed and nothing on disk changes |

**Idempotent by construction:** a dry run computes every file change (and its diff) the same way `--write` does, just without calling the filesystem — so running `--write` a second time against an already-migrated file leaves it byte-identical rather than erroring or double-rewriting.

**Shape C is always refused.** A Shape C agent's `/runs` route streams Server-Sent Events directly from the route handler — which frame to yield next, and when to fall back to a plain JSON error, is control flow the codemod cannot rewrite mechanically. `analyze` marks a Shape C plan `codemod.eligible = False` with that reason, and `transform` refuses it outright rather than attempting a partial rewrite; it needs a manual pass instead.

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Dry run completed, or `--write` applied every planned action |
| `2` | Refused — Shape C (or an unclassified shape), an action in `plan.codemod.actions` with no handler in this SDK version, or the plan file could not be read |
| `3` | The `migrate` extra is not installed (`libcst` missing) |

## `verify`

Converts each of the agent's own pre-migration fixtures (`tests/test-data/*.json`) into the A2A `SendMessage` request an equivalent caller would send today, fires it at a live agent process at `--base-url`, and asserts the resulting task reached the expected state. It also checks the live Agent Card against `manifest.json` — same skill set on both, or the run fails. Nothing here imports the agent under verification except the card check itself; every message check goes over the wire through `Client`, exactly like a real caller.

```bash
python -m novaos_client_sdk.migrate verify <path> [--base-url URL] [--plan PLAN.json] \
    [--expect skill=STATE[,skill=STATE...]] [--stream skill[,skill...]] [--out DIR] [--timeout SECS]
```

| Flag | Default | Meaning |
|---|---|---|
| `path` | — | Agent directory to verify |
| `--base-url` | `http://127.0.0.1:8001` | Where the migrated agent is running |
| `--plan` | none | Path to `<agent>.plan.json`; without it, `verify` re-analyzes the agent tree itself (safe post-migration — a fully migrated `server.py` just reports `Shape.UNKNOWN`) |
| `--expect` | none | Comma-separated `skill=STATE` overrides |
| `--stream` | none | Comma-separated skill names to also probe with `SendStreamingMessage` |
| `--out` | none | Directory to write `<agent>.verify.json` / `<agent>.verify.md` into |
| `--timeout` | `30.0` | Per-request timeout, in seconds |

Every fixture defaults to expecting `TASK_STATE_COMPLETED`. Three ways to override it, in increasing precedence: a checked-in `tests/test-data/verify.json` keyed by fixture filename (durable, e.g. `{"pause-request.json": {"expected_state": "INPUT_REQUIRED", "stream": true}}`), then `--expect` on the command line, keyed by skill. State names match case-insensitively and with or without the `TASK_STATE_` prefix. Streaming is opt-in per skill via `--stream` or `"stream": true` — a probed stream must produce at least one frame and close on a terminal or interrupt state.

**Exit codes:**

| Code | Meaning |
|---|---|
| `0` | Every skill check and the card check passed |
| `1` | At least one skill assertion failed, or the card and manifest disagree on the skill set |
| `2` | Could not verify at all — no fixtures found, or a fixture/plan file could not be read |

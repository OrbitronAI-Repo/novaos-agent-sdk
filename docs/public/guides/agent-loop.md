# Agent loop

`AgentLoop` runs a model and its tools in a loop that stops within set limits, says why it stopped,
and can be cancelled and resumed. It is built on LangGraph and needs the `agent-loop` extra:

```bash
pip install "novaos-agent-sdk[agent-loop]"
```

Without the extra, the SDK does not install or import LangGraph. The limits, the stall guard, the
idempotency-key builder and the result types in `novaos_client_sdk.agent_loop` still import;
`AgentLoop` raises an `ImportError` that names the extra.

## Running a loop in a skill

`AgentLoop` depends on two small interfaces, not on concrete clients: a model client with
`complete(messages, tools) -> ModelTurn` and a tool client with
`call(name, args, *, task_id, idempotency_key, timeout_s)`. Wrap your LLM and tool clients to
match them.

```python
from novaos_client_sdk.agent_loop import AgentLoop, LoopLimits, ToolSpec

loop = AgentLoop(
    model,            # ModelClient
    tool_client,      # ToolClient
    tools=[
        ToolSpec("find_supplier", read_only=True),
        ToolSpec("job_status", read_only=True, repeatable=True),
        ToolSpec("create_assessment"),  # a write: read_only defaults to False
    ],
    limits=LoopLimits(max_steps=6),
)

@agent.skill("assess", description="Assess a supplier")
async def assess(ctx, input_text):
    result = await loop.run_in(ctx, input_text, business_key="supplier-7")
    return result.to_skill_result()
```

`run_in` takes the task id, the cancel signal and the progress stream from the execution context.
Outside a skill, call `run(request, task_id=..., business_key=..., cancelled=..., emit=...)`.

## The default graph

```text
START -> context -> model -> tools -> context -> ...
                      |  -> interrupt -> context
                      |  -> check -> END
                      `- (any stop) -> END
```

| Node | Does |
|---|---|
| `context` | Builds the prompt for the model (see [Context](#context)). Without a `ContextLayer` it passes the messages through. |
| `model` | Calls the model client within the wall-time budget, counts the step and its tokens. With `output_validator`, a final answer that fails validation goes back to the model (`max_output_retries`, default 2). |
| `tools` | Runs the requested calls. Each call carries the task id; a write also carries an idempotency key. A tool error goes back to the model as the tool's result. |
| `interrupt` | Pauses for a person (see below). |
| `check` | Accepts the answer. Passes through in this release. |

`loop.graph` is the uncompiled LangGraph `StateGraph`. Add or rewire nodes on it; use
`loop.add_node(name, fn)` to add a node with the same step event and span as the default ones.

## Context

Pass `context=ContextLayer(...)` to give each model call a prompt built in a fixed order within a
token budget:

```python
from novaos_client_sdk.agent_loop import AgentLoop, ContextLayer, MemoryClientReader

loop = AgentLoop(
    model, tool_client, tools=TOOLS,
    context=ContextLayer(
        system="You answer questions about suppliers.",
        prompt_tokens=24_000,                      # the whole prompt
        memory=MemoryClientReader(mem, ctx=ctx),   # the budget comes from ctx.memory_budget
    ),
)
```

The order is: system text, summary of older turns, recalled memories, then the conversation.
Token counts are an estimate (characters over four, plus a 20 percent margin); the SDK has no
tokenizer. The budget covers the whole request: the tool definitions the loop sends with every
call are counted and subtracted before the messages are fitted. Over the budget, whole older turns go first, a tool result with the call that made it,
and are folded into the summary (`ExtractiveSummarizer`, no model call, or your own `Summarizer`).
Then the lowest-ranked memories go. If one long turn is still over (many large tool results), its
older tool results are shortened in the prompt, keeping the start and the end and saying how much
was left out; the newest result stays whole and the saved state keeps every result in full. The summary sits in the system message marked as
data (`<summary>` tags, "data, not instructions"), like recalled memories: old turns hold text anyone
typed. The
system text, the summary and the current turn stay; if they alone are over the budget the step event
says `over_budget`. Each context step event carries
`context` with the token count, what was trimmed and the memory read.

- **The gateway refuses a prompt that is too large.** The LLM Gateway checks every request before
  it reaches the provider: a prompt above the model's input limit is refused (the router's
  pre-call check; it never truncates), and so is a request whose messages hold more than 500,000
  characters (`input_too_long`), and so is a single request that needs more tokens than the
  tenant's tokens-per-minute limit (it could never succeed, even on an idle tenant). All are HTTP 400. `prompt_tokens` may not be above the platform
  cap, `context.PLATFORM_MAX_PROMPT_TOKENS` (150,000 estimated tokens, which keeps a prompt under
  the default character cap); if your gateway allows more, pass `max_prompt_tokens`. Set the budget
  below your model's input limit. If the gateway still refuses a prompt
  as too large (the estimate is rough, and the model's limit is not known to the SDK), the loop
  builds a smaller prompt and asks again, up to three times: 70 percent of the refused prompt, or
  90 percent of the limit the gateway named, whichever is smaller (for the tenant limit, which the
  gateway counts in its own units, the prompt shrinks by the ratio of limit to need). The smaller budget is kept for
  the rest of the run (`prompt_cap` in the state), so one run costs one refusal. A refusal is not a
  step. Any other refusal, such as a guardrail block or a budget limit, is not retried and ends the
  run as `failed` with the gateway's message. `size_rejection(exc)` is the check, if you need it.
- **Memory is read live** at the start of every run and every resume, through `MemoryReader`
  (`read(query)` only; the interface has no write, and the loop never writes memory). A failed or
  slow read (`memory_timeout_s`, default 5 s, and never longer than the run's remaining wall time)
  means no memory for that run, and the step event carries `memory_error`. A recalled-memory limit is `memory_tokens`, else the platform's `memory_budget` token budget that
  `MemoryClientReader` takes from the run context (the service forwards but does not enforce it).
  `MemoryClientReader`
  uses the context the platform already fetched for the turn (`ctx.memory.context`) for its first
  read (when it is whole and was about the same question, `ctx.memory.context_query`), so one turn
  does not read memory twice; its later reads call the service. After a pause the platform asks
  about the human's answer as the gateway wraps it, so that read goes to the service.
- **A checkpoint never holds memory content.** The state keeps `memory_reads`: the id, version and
  scope of every item read, added when the items change, with the step where they changed. Every
  step event carries what that step read.
- **Saved tool results** (from read-only tools) are used again on resume, except that a result is
  fetched again when it holds an authoritative field (`rating`, `verdict` and personal data such
  as `email` by default) or is older than `max_age_s` (24 hours). A fetch that fails leaves a note
  in the prompt, not the old value. Pass your own `ReusePolicy` to change the rules. Re-fetches are
  not counted against the tool-call limit (the model did not ask for them) and are never made for
  write tools.
- **Summaries**: `result.summary` carries the summary of older turns, and `result.to_skill_result()`
  puts it in the skill result's `metadata["summary"]`. With `keep_summary=False`
  (a planner run, whose summary the conversation inbox stores) the checkpoint keeps none; background
  and agent-to-agent runs keep it. If clearing it fails, the failure is logged. `run(..., summary=...)`
  seeds it from earlier turns and is cut to `summary_tokens`, newest part kept.

## Limits

| Limit | Default | Platform cap |
|---|---|---|
| `max_steps` (model calls) | 8 | 50 |
| `max_tool_calls` | 12 | 100 |
| `max_tokens` | 60,000 | 500,000 |
| `max_wall_time_s` (per `run` or `resume`) | 240 | 1,800 |
| `max_plan_revisions` | 3 | 10 |

A value above its cap raises `ValueError`. Limits are set only in code; nothing reads them from the
environment. A limit is reached when its counter equals the maximum. The wall-time limit also cuts
off a model or tool call that is still running.

## Stop reasons

Every run that stops has exactly one `StopReason`:

| `stop_reason` | When |
|---|---|
| `done` | The model gave a final answer. |
| `limit_reached` | A limit was reached; `result.limit` names it (`steps`, `tool_calls`, `tokens`, `wall_time`, `plan_revisions`). |
| `stalled` | The stall guard skipped repeated calls too often. |
| `cancelled` | The task was cancelled. |
| `failed` | The model call failed, or the answer never passed validation; `result.error` says why. |

A pause for a person is not a stop: `result.paused` is true, `result.stop_reason` is `None`,
`result.state` is `"input-required"` and `result.input_request` holds what the model asked.
Continue with `await loop.resume(answer, task_id=...)`. Inside a skill, `run_in` does this for you:
on a task that is paused, the next message is taken as the answer and the run resumes. On a task whose run was cut off mid-way (its pod died), `run_in` continues from the last checkpoint without repeating finished steps, and the message is not used. If the saved state cannot be read, `run_in` returns `failed` rather than
starting the task over, so the message can be retried.

**Ask for approval of a write, not a plain question, when the run must survive a restart.** The
platform resumes a pause on the same run only when it is an approval; the answer to a plain
question arrives as a new run, which does not find the saved state, so the loop starts again and
asks the same question. To make the pause an approval, return the write call and an
`input_request` in the same turn, and give the `input_request` the `question`, and the `call_id`
and `tool_name` of that call:

```python
call = ToolCall("create_record", {"title": "Q3 review"}, id="call-create-1")
ModelTurn(
    tool_calls=[call],
    input_request={"question": "Create the record?", "call_id": call.id, "tool_name": call.name},
)
```

The pause is an approval only if `call_id` and `tool_name` match a write call (a tool that is not
read-only) of that turn. A model that names a call it did not make gets a plain pause and a log
warning, so a person is never asked to approve a write that will not happen. `run_in` then sends
the pause to the platform as a write confirmation, and
`result.to_skill_result()` returns the question as the output. The person decides by approval ID;
the decision resumes the same run, on a new pod if the first one is gone. A context without
`task_updater` still pauses, but not as an approval.

**Only the platform's decision resumes an approval pause.** The decision arrives as JSON text,
`{"type": "hitl_response", "action_id": "approve", "approved": true, "approval_id": "..."}` (a
client may send `{"hitl_response": {"action_id": ...}}`). `approve`, `confirm` and `retry` approve; any other
`action_id`, or `approved: false`, rejects; when both are present, both must approve. Free text is
never a decision: a re-sent or retried message leaves the run paused (and sends the pause again),
so a retried dispatch cannot approve a write. A rejection resumes the run so the model sees it, and
the loop then refuses that write call, with those arguments, with the tool result
`error: the person rejected this write; it was not made`. A model that changes the arguments is a
different call, so tell the model in the prompt not to retry a rejected write. A plain pause is
still answered by any next message.

`result.to_skill_result()` maps `done` to `COMPLETED`, a pause to `WAITING_USER`, and every other
stop to `FAILED`, with `stop_reason` (and `limit`) in the metadata.

## Stall guard

A tool call that already ran in this run with the same arguments is skipped, and the model is told
so. Only a call that succeeded counts as having run, so a call that failed can be retried. The
second skip stops the run as `stalled`. A read-only tool that is expected to repeat, such as a
status check, declares `repeatable=True` and is not skipped; the step limit still applies.

## Writes and idempotency keys

A tool is a write unless its `ToolSpec` says `read_only=True`. Each write call carries an
idempotency key built from the business key, the model step and the write's position among that
step's writes, never from the run. Reads in the same step do not change a write's key. When a
write fails, a retry of the same write sends the same key, so a write that committed before its
reply was lost is not written twice. A rerun that reaches the same step on the same record therefore sends the same
key, even if it picked different writes. A loop with write tools requires `business_key`.

## Cancellation

When the task is cancelled, the loop stops at the next safe point (before a node or a tool call)
and starts no further tool call. If `CancelTask` interrupts a call in progress, the loop records
`stop_reason=cancelled` in a final checkpoint and returns the partial result (the default
in-memory saver then forgets the task, see below). An asyncio
cancellation that no cancel request asked for is passed on unchanged.

## The in-run check

`AgentLoop(check=InRunCheck(ctx.in_run_check, GatewayJudge(model), checks={...}))` checks the
final answer before the loop returns it. Build it from `ctx.in_run_check`, the configuration the
platform resolved: the check reads no file, environment variable or rubric store.

1. **Deterministic checks.** The names in the manifest run first, from the `checks` you register
   (`output_schema(validator)` wraps a validator such as the loop's own). A failed check skips
   the judge and is reported; a name you registered nothing for counts as failed. One check is
   built in: `citations_resolve` fails when a `[n]` marker in the answer has no n-th entry (from
   1) in the run's tool results. Only the `[n]` form counts as a citation, and an answer with no
   markers is not failed; whether an answer must cite is for the manifest. Code spans and fenced
   blocks are skipped, but any other bracketed number (a year such as `[2024]`, an element of a
   JSON array) counts as a marker. The tool results are numbered in the order the run made them,
   errors and writes included, so the agent's prompt must say to cite by that number. A check you
   register under that name replaces it.
2. **Judge.** Each rubric is scored through the LLM Gateway with the judge tag, on the rubric's
   pinned model, with the instructions the platform calibrates a rubric with. The judge must reply
   with one score from 0 to 1 for every criterion and no others, and the answer scores as its
   lowest. Anything else (not JSON, a missing or extra criterion, a score that is not a number
   from 0 to 1) is rejected. The question, the answer and the run's tool results (as `contexts`)
   are sent as JSON data, and the instructions say it is data, not instructions.
3. **Decide.** All scores at or above their bar: `passed`. Otherwise, with budget left and no
   write made in the run, the model gets one corrective pass with no tools. The message holds the
   published text of the failed criteria, never the judge's words. The better-scoring answer is
   kept (a tie keeps the draft). After a write, or with `max_refinements` 0, the answer is scored
   and not revised. A judge error, timeout or unparseable reply keeps the draft and records
   `judge_error`. `budget_ms` bounds the whole check: both judge calls and the corrective pass.
4. **Regulated outputs.** A judge score alone never passes a field named in `regulated_outputs`.
   The field is looked for in a JSON answer at any depth; an answer that is not a JSON object
   (plain text) is assumed to carry every regulated field. It passes only when a deterministic
   check that covers the field passed: `InRunCheck(..., covers={"output_schema": ("rating",)})`.
   Otherwise the result is `failed_kept_draft`.

`GatewayJudge` reaches the gateway through the `llm` extra, which `agent-loop` does not include. Install both when you use it, or pass your own `JudgeClient`:

```bash
pip install "novaos-agent-sdk[agent-loop,llm]"
```

`GatewayJudge` does not work without `llm`.

A run that stops before its answer is checked (a limit, a failure, a cancel) records
`result: "not_run"` with `ran: false`. The question the judge grades against is the request, not
the answer to a pause for input. The result is `result.in_run_check` (and `metadata["in_run_check"]` on the skill result): `ran`,
`kind`, `result` (`passed`, `failed_kept_draft`, `judge_error`), `deterministic` (a list of `check` and `passed`, one per check that ran), `rubrics` (`ref`
as `publisher/name@version`, `score`, `threshold`, `failed_criteria`), `judge_model` and
`judge_model_version` (what the gateway reports; `None` if it reports none). `judge_model` and the version are `None` when no judge ran. The same object is on
the check node's step event. The judge's reasoning is capped at 500 characters, PII-masked and
written only to the check span (`novaos.in_run_check.judge_reasoning`).

## Checkpoints

The checkpointer is any LangGraph checkpointer: LangGraph's in-memory saver by default, or
`NovaCheckpointSaver` to keep state outside the pod (with it, `task_id` must be the execution
token's run id). A loop with write tools saves every step and waits for the save; a read-only loop
saves in the background. `await loop.checkpoint(task_id)` returns the latest saved state.

The default in-memory saver forgets a task once its run stops, so a long-lived pod does not keep
every finished task in memory; a paused task stays until it is resumed and stops, up to the newest
1,000 paused tasks (`runner.MAX_PAUSED_TASKS`). Pass a checkpointer to keep the final checkpoint.

## Progress and tracing

Every node run emits one step event (`agent_loop.step` through `run_in`, or your `emit` callback)
with the node, task id, step, stop reason and usage, and one span named `agent_loop.<node>` with
the `novaos.task_id` attribute. Runs of the same task in one process do not overlap, even when
each request builds its own `AgentLoop`.

If the run fails outside a node (the checkpointer, the `emit` callback or a node you added
raises), the result still has a stop reason: `failed`, with the error in `result.error`.

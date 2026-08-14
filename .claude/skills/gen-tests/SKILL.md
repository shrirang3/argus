---
name: gen-tests
description: Generate pytest test cases for a given file, function, or endpoint by reading the code and existing tests/ conventions first, then writing tests that follow the same style (fixtures, asyncio_mode=auto, naming). Use when the user asks "write tests for X", "generate test cases", "add coverage for", "what's untested here", or wants Claude to produce tests rather than hand-write them.
---

# Generate tests

Tests generated without reading the target code first are guesses. Read before writing,
every time — no exceptions for "simple" functions.

## Process

1. **Read the target file completely.** Not an excerpt — every branch, every error path.
2. **Read one existing test file in the same directory tier** (`tests/unit/` for a unit,
   `tests/integration/` for a service) to match fixture style, naming (`test_<behavior>`,
   not `test_<function_name>`), and how async is handled (`asyncio_mode = "auto"` in
   `pyproject.toml` — no `@pytest.mark.asyncio` needed, define `async def test_...`
   directly).
3. **List the branches before writing a single test** — happy path, each error path,
   each boundary (empty input, max size, duplicate `event_id`, concurrent write). Missing
   branches are the actual bug generated tests usually have.
4. **Write one test per behavior, not per function.** A function with 3 branches gets
   3 tests, not 1 test with 3 asserts.
5. **Run them** (`uv run pytest tests/... -v`) before reporting done. A test that wasn't
   run is a claim, not a result.

## What to prioritize, in this codebase specifically

- **Idempotency / dedup** — anything touching `event_id` + `ON CONFLICT DO NOTHING`
  (worker writes). Test that redelivering the same event is a no-op, not a duplicate row.
- **Redaction** — PII redaction runs twice (SDK + ingestion edge) by design. Test each
  layer independently; a test that only exercises the combined pipeline can't tell you
  which layer would fail if the other were removed.
- **Non-blocking guarantee** — the SDK's emit path must never await the network call on
  the request path. If ingestion is down/slow, the test should assert the caller doesn't
  block, not just that no exception propagates.
- **Ordering** — `messages.seq` uniqueness per `conversation_id`; concurrent inserts
  should fail on the constraint, never both silently succeed.
- **Boundary conditions on money/tokens** — zero tokens, unpriced model (`unpriced`
  counter in the dashboard overview exists because this happens for real), negative or
  missing `latency_ms`.

## Output

Write the test file directly into `tests/unit/` or `tests/integration/` matching the
existing layout — don't create a new top-level test directory. Show the branch list you
identified in step 3 in the chat response before the code, so the user can spot a missed
case before running anything.

After writing, run the new tests and paste the actual pass/fail output. If something
fails, fix the code or the test — whichever is wrong — and say which.

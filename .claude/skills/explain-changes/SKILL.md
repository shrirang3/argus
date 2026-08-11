---
name: explain-changes
description: Produce a full walkthrough of code that was just written or changed — API surface inventory, edit-by-edit purpose, the core concept behind each decision, and an end-to-end data-flow trace. Use after ANY code change in this repo (new files, edited files, new endpoints, refactors), and whenever the user asks "explain what you did", "walk me through", "what APIs did you define", "no vibe coding", or "explain step by step". The user is learning this codebase for interviews, so shipped code is not done until it is explained.
---

# Explain changes

The user is building this repo as an interview deliverable. Code they cannot explain
is worthless to them. **A change is not complete until the walkthrough exists.**

Write the walkthrough in the chat response, immediately after the work, before asking
what to do next. Do not wait to be asked.

## Hard rules

- **No unexplained code.** Every file created or edited gets covered.
- **Why before what.** The diff already shows what. Explain the reasoning.
- **Name the concept.** Each non-obvious decision gets its underlying idea named
  explicitly (idempotency, backpressure, context propagation, layer caching…), because
  that is the word an interviewer will use.
- **State what you did NOT do**, and why. Omissions are decisions.
- **Flag anything temporary** — stubs, placeholders, hardcoded values — with the phase
  that removes it.
- **Be honest about what was verified** vs assumed. Never imply a check you didn't run.

## Required structure

Adapt the sections to what actually changed — skip any that don't apply — but keep this
order.

### 1. API surface

Only if endpoints changed. One table, complete:

| Method | Path | Body | Returns | Status codes |
|---|---|---|---|---|

Then, for any endpoint with non-obvious behaviour, a short paragraph: what it does, and
the one design decision inside it worth defending.

### 2. Files: what changed and why

One subsection per file, in dependency order (data layer → logic → transport → UI), not
alphabetical. For each:

- **Purpose** — one line on why this file exists
- **Key decisions** — the choices a reviewer would question, with the reasoning
- **Core concept** — the named idea, explained in plain language, ideally with a
  concrete analogy

Quote only the lines that carry a decision. Never paste whole files back.

### 3. Data flow

Trace one complete request end to end, in order, naming the function at each hop:

```
browser click → fetch() → route handler → service → DB → response → DOM
```

The point is to show where the boundaries are and what crosses them.

### 4. Concepts introduced

Table of anything new: term, plain meaning, why it's here. This is the interview-revision
layer — assume the term will be asked about cold.

### 5. Verified / not verified

Two explicit lists. What was actually run and its result; what was not checked and how
it would be checked. Never blur these together.

### 6. Deliberately not done

Scope boundaries and temporary shortcuts, each with the phase or condition that resolves
it.

## Also append to the journal

After writing the walkthrough in chat, append the same material — expanded, with more
theory and analogies — to `docs/interview-notes.md`, which is gitignored local study
material. Match its existing voice: technical, analogy-driven, organised by concept
rather than by file. Add to the rapid-fire Q&A table at the end for anything an
interviewer would plausibly probe.

## Tone

Explain to a competent engineer who has not seen this code. No hedging, no filler, no
restating the obvious. Tables and short paragraphs over prose walls. If a decision was a
close call, say so and give the deciding factor — those are the strongest interview
answers.

# Sky Backcraft — project agent task contract

Mapping, implementation and test/runtime verification use `gpt-6-luna` with `max`
reasoning. Code review uses `gpt-6-astra` with `high` reasoning, per the user's
2026-10-20 routing change. Main keeps its existing model, owns decomposition and
integration, and makes final acceptance decisions. At most four children may run
concurrently. Children do not delegate or silently select a different model;
material ambiguity returns to Main. The legacy contract filename is retained for
existing references.

Use these project roles instead of global role profiles for this repository:

| Role | Responsibility |
| --- | --- |
| `backcraft_explorer` | Read-only current-source, ownership and dependency mapping |
| `backcraft_backend` | Bounded Rust, SQLite-owner, job and MCP implementation |
| `backcraft_gui` | Bounded native AppKit GUI and read-projection implementation |
| `backcraft_verifier` | Independent tests, runtime and native GUI evidence |
| `backcraft_reviewer` | Read-only code, contract and regression review; Astra / high |

Main assigns build-error repair and packaging to the relevant Backend or GUI
writer, or performs integration directly; there is no separate packaging role.

Capacity is a ceiling, not a requirement to launch four workers. Dispatch only
independent, useful packets; use one active writer per file or authority boundary.
State the observable objective, accepted decisions, relevant source/snapshot,
owned files, non-goals, acceptance criteria, authorized effects, required checks,
stop conditions and escalation target. Do not send the full conversation when a
small current packet suffices. Use the repository's CodeGraph first where indexed.

Follow current user instructions, repository `AGENTS.md` and the applicable
locally provisioned `.agents/` Harness. Load only relevant rule families. Preserve
dirty/untracked work and the SSD Rust build policy. Private Harness material stays
local. Do not change existing tool endpoints, credentials or approval settings.

SQLite remains owned by `DatabaseOwner`; do not introduce another runtime writer.
Keep immutable run/model/policy-revision/dataset identities, causal/PIT boundaries,
exact economic decimals and unknown-versus-zero distinctions. Native clients read
Rust projections; Rust owns exchange credentials, signing and effect admission.
Historical replay, jobs, paper sessions, exchange reads and trading authorization
remain distinct. An absent persistent paper session is unavailable, never active.

Within GUI work, research-note/journal features are excluded by the user's latest
request. Preserve strategy-first navigation, independent run/market curves, privacy,
provenance, freshness and native accessibility.

No real orders/cancellations/transfers/withdrawals, automatic trading enablement,
policy hard-delete, paused-collector resume, original ledger/dataset deletion,
credential mutation, publication or commit/push is authorized by a role profile.
Escalate a missing contract or ownership conflict to Main with evidence.

Verification must not edit production source, tests, fixtures, goldens, configuration,
acceptance criteria or dirty/untracked evaluation inputs. Only authorized temporary
outputs may be produced. A changed relevant input invalidates the affected evidence.
Run focused checks and required gates; broaden only when new changes or failures
justify it. Distinguish source, unit, integration, native GUI, runtime, performance
and live-provider proof. Return actual PASS/FAIL/INCONCLUSIVE with paths, commands,
snapshot, missing checks and remaining risks. Worker self-report is not final proof.

Source: Alexandria `Jobs/agent_package/GPT-6/` role/task/model/installation contracts.
Source identities and hashes are recorded in `provenance.json`. The optional draft
bounded-work skill is not installed as a universal workflow or promoted to canonical
engineering authority.

# Project subagents

This project defines five custom role files in `agents/`. Mapping, implementation
and test/runtime verification use GPT-6 Luna / max. Code review uses GPT-6 Astra /
high, per the user's 2026-10-20 routing change.
The child concurrency limit is four, excluding Main; nested delegation is disabled.
The current Main model and global configuration are preserved.

Load `LUNA_TASK_CONTRACT.md` before delegation. Main assigns disjoint owned surfaces
and remains responsible for integration and final acceptance.

- `backcraft_explorer`: source and authority mapping, read-only.
- `backcraft_backend`: Rust/SQLite/job/MCP implementation.
- `backcraft_gui`: native macOS GUI and read projection implementation.
- `backcraft_verifier`: independent validation; generated test outputs only.
- `backcraft_reviewer`: code/contract/regression review, read-only; Astra / high.

Project role model/effort pins prevent older global role defaults from substituting
another model. Other global profiles may remain discoverable; project delegation
uses the five names above. No fixed fanout is required. If the current session has
not loaded the new reviewer role, Main may explicitly dispatch a default child
with `gpt-6-astra` / `high` and the reviewer contract; this is an explicit routing
adapter, not a fallback to Luna. Do not change the implementation/verifier pins.
Build fixes and packaging belong to the assigned Backend/GUI writer or Main.

Sandbox settings express role intent. Effective session permissions and actual
loaded model settings require runtime evidence; syntax validation alone is not
proof of sandbox isolation or a successful model invocation. Existing running
children keep their prior settings. New project sessions load this configuration.

Local Alexandria installation provenance is in the ignored `provenance.json`.
Verification results and unverified runtime lanes are in the ignored
`.omx/evidence/project-agents/` directory. Neither is required in a clean checkout.

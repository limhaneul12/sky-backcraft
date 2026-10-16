# Project subagents

This project installs four custom role files in `agents/`. All use GPT-6 Luna / max.
The child concurrency limit is four, excluding Main; nested delegation is disabled.
The current Main model and global configuration are preserved.

Load `LUNA_TASK_CONTRACT.md` before delegation. Main assigns disjoint owned surfaces
and remains responsible for integration and final acceptance.

- `backcraft_explorer`: source and authority mapping, read-only.
- `backcraft_backend`: Rust/SQLite/job/MCP implementation.
- `backcraft_gui`: native macOS GUI and read projection implementation.
- `backcraft_verifier`: independent validation; generated test outputs only.

Project role model/effort pins prevent older global role defaults from substituting
another model. Other global profiles may remain discoverable; project delegation
uses the four names above. No fixed four-way fanout is required.
Build fixes and packaging belong to the assigned Backend/GUI writer or Main.

Sandbox settings express role intent. Effective session permissions and actual
loaded model settings require runtime evidence; syntax validation alone is not
proof of sandbox isolation or a successful model invocation. Existing running
children keep their prior settings. New project sessions load this configuration.

Local Alexandria installation provenance is in the ignored `provenance.json`.
Verification results and unverified runtime lanes are in the ignored
`.omx/evidence/project-agents/` directory. Neither is required in a clean checkout.

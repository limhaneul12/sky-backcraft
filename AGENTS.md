# Repository Guidelines

## Execution and Approval
- For requested repository changes, perform reversible in-scope local edits and relevant non-destructive validation without unnecessary confirmation.
- Preserve unrelated dirty worktree changes. Do not use broad reset, clean, revert, checkout, or stash as routine cleanup.
- Ask before external publication/deployment, credential mutation, destructive data actions not already requested, commit/push, or irreversible scope expansion.

## Applicable Instructions
Precedence: platform/safety → current user task → closest `AGENTS.md` → broader repository instructions → adopted Engineering Harness profile/rules → task-relevant Skills → live code conventions.

Load only the rule families relevant to the touched surface. PRDs and notes are task inputs, not engineering-rule authority unless explicitly designated.

## Engineering Harness
Engineering testing, verification, change discipline, solution minimality, and language invariants follow the repository Harness under `.agents/`.

The `.agents/` bundle is private, locally provisioned, and excluded from Git and its history. Preserve its local rules, skills, and documents; do not publish them. Clean checkouts and CI do not require it.

Rust work:
- `.agents/rust_dev_harness/PROJECT_PROFILE.md`
- `.agents/rust_dev_harness/HARNESS.toml`
- `.agents/rust_dev_harness/rules/README.md`
- load only applicable normal/type/async rules, activated profiles, and Skills.

Shared contracts: `.agents/shared/contracts/`, `.agents/shared/skills/minimal-engineering/SKILL.md`.

Do not duplicate Harness doctrine in AGENTS. If a mechanical verifier is stricter than its governing rule/profile, treat it as Harness drift before distorting production design.

## Toolchain
Execution authority is the repository-declared toolchain: `rust-toolchain.toml` pins channel `1.98.0`. Ambient PATH cargo/rustc (currently a stale rustup default) is diagnostic only; inside this repository they resolve through the rustup proxy to the declared channel.

Local build storage: point `CARGO_TARGET_DIR` at `/Volumes/256.SSD/RustBuild/sky-backcraft` for local runs per the user's SSD storage policy. This is a machine-local preference, not a CI requirement.

## Build and Verification
Canonical entrypoints:
- `cargo xtask rules`
- `cargo xtask fmt`
- `cargo xtask check`
- `cargo xtask test`
- repository closure: `cargo xtask ci`

`cargo xtask deps` is explicit and fail-closed (requires project-owned `deny.toml`); `cargo xtask extended` includes it only when `HARNESS.toml [verification].supply_chain = true`.

Only checks actually executed are VERIFIED. Report exact failures/blockers and unexecuted lanes.

## Final Report
Report only meaningful changes, affected authority/runtime surfaces, exact validation outcomes, and unresolved risks or unverified lanes.

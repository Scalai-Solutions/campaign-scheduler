---
name: safe-operations
description: Standing guardrails for git and deployment in Voone repos. Use before pushing, merging, opening PRs, committing, deploying, running terraform, or any irreversible/outward-facing action. Always applies.
---

# Safe Operations — Standing Guardrails

These rules always apply across every Voone repo (v3 root and all `services/*`). They are
defaults, not absolutes: the user can override any of them per task with an explicit
instruction.

## Git

- **Never push to `main` unless explicitly told to.** Default to feature branches.
- Do not commit or push at all unless the user asks. When work is done, leave it staged or
  uncommitted and say so — let the user decide when to commit.
- Never force-push, rewrite published history, or delete branches without an explicit ask.
- If you're on `main`/`master` and need to commit, create a branch first.
- End commit messages with the required `Co-Authored-By` trailer.

## Deployment

- **Never deploy to prod unless explicitly told to.** This includes `deploy-prod` GitHub
  workflows, `terraform apply` against prod, pushing prod images, ECS/service restarts, and
  any prod infra mutation.
- Deploying to stage/dev is also opt-in — confirm scope before running deploy scripts.
- Prefer dry runs (`terraform plan`, `--dry-run`) and show the plan before any apply.

## Data & infrastructure

- Production database access is read-mostly and time-boxed — use the `database-access`
  skill's script, which auto-revokes its temporary access rule. Never leave a DB security
  group opened.
- Never widen a security group to `0.0.0.0/0`, disable auth, or copy secrets out of Secrets
  Manager / instances into the repo.
- Before deleting or overwriting anything you did not create, inspect it and surface what
  you found instead of proceeding silently.

## When in doubt

State what you would do, note that it's outward-facing or hard to reverse, and ask for
explicit confirmation rather than assuming approval carries over from a previous step.

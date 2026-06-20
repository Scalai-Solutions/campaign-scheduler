---
name: git-workflow
description: Branching, PR, and deploy model for campaign-scheduler. Use before creating branches, opening PRs, or reasoning about how this service reaches stage/prod.
---

# Git & Deploy — campaign-scheduler

Full model: the **v3-root `git-workflow` skill**. Guardrails: `safe-operations`.

- **Default branch: `dev3`.** Branch `feature/*`/`fix/*` off `dev3`, PR back into `dev3`.
- **`dev3` is the stage source**; promote `dev3 → main` (PR) for prod.
- **Deploys orchestrated from `scalai-v3`** — pushing this repo's `dev3` deploys nothing:
  - push `scalai-v3` `stage` → builds from **`dev3`** → stage (**EC2 Docker host**).
  - push `scalai-v3` `main` → builds from **`main`** → **PROD** (ECS `voone-prod-campaign-scheduler`).
- No own workflow and **no `stage` branch** (not needed under the dev3-as-stage model).
- Never push `scalai-v3` `main` without explicit go-ahead.

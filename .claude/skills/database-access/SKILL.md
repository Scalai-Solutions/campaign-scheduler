---
name: database-access
description: Connect to the live stage/prod MongoDB for campaign-scheduler. Use when asked to inspect campaign definitions, runs, executions, step/node runs, batch dispatches, scheduled tasks, or campaign chat sessions in the stage/dev or production database.
---

# Database Access — campaign-scheduler

Use the canonical secure helper (authorizes only your current IP, connects, revokes on
exit). Full mechanism, security model, and environment table are in the **v3-root
`database-access` skill**.

```bash
/Users/weekend/scalai/v3/scripts/db-connect.sh <stage|prod> [database] [--eval '<js>'] [--yes]

db-connect.sh prod scalai_db --eval 'db.campaignruns.countDocuments()'
```

## Databases this service touches

The campaign execution engine. Reads/writes **`scalai_db`** and **per-tenant
`subaccount_{id}`**:

- Campaign models: `CampaignDefinition`, `CampaignRun`, `CampaignExecution`,
  `CampaignNodeRun`, `StepExecution`, `BatchDispatch`, `ScheduledTask`,
  `CampaignChatSession`, `NextStepIntent`, `RetellEvent`.
- Reads `phonenumbers` (dispatch targets).
- Uses **Redis** for scheduling/queues (`CAMPAIGN_SCHEDULER_DB_NAME` selects the Mongo DB).

See also: `safe-operations`.

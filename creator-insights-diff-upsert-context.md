# Creator Insights Diff-Based Upsert — Project Context

Context doc for AI agent sessions. Covers the project goal, architecture, code locations, the
rollout, every problem that came up and how it was resolved, decisions, and open items.
Repo: `Patreon/data-eng` (checked out at `/Users/gmahajan/GitHub/data-eng`).

---

## 1. One-liner

Making the Creator Insights Elasticsearch pipelines more efficient by writing only the rows that
changed on each upsert, instead of rewriting the full lookback window every run. A Delta "state
replica" table mirrors each ES index and is used to compute the diff.

## 2. Background: how it works today (the "rolling rewrite")

- Creator Insights jobs compute analytics and write to versioned Elasticsearch (ES) indexes that
  power creator-facing dashboards. Indexes use `-search` / `-write` aliases; a **rebuild** creates a
  fresh versioned index and swaps the aliases.
- The framework (`ESWriteJob`) runs in modes selected by a Databricks `mode` widget: `rebuild`,
  `upsert`, `test`, `write_to_dbx`, `split`.
- **Rolling rewrite (current upsert):** each run recomputes a recent lookback window and rewrites
  *all* of it to ES, keyed by the `id` field (upsert-by-id: existing docs overwritten, new docs
  inserted). It re-writes unchanged rows too — that redundancy is what this project removes.
- **Deletes are out of scope.** Upserts never delete; stale docs are only cleared by a rebuild
  (fresh index + alias swap). This is a deliberate, pre-existing design decision.

## 3. The new approach (diff-based upserts)

- Keep a Delta **state replica** table that mirrors the ES index.
- Each upsert: write the window to a **staging** table, compute the **diff** (staging vs replica),
  send only changed rows to ES, and merge the diff into the replica.
- **Two-phase rollout per job:**
  - **Shadow** (`write_diff_to_es=False`): ES still gets the full rolling rewrite (no creator-facing
    change), but the diff is computed and merged into the replica so it can be validated.
  - **Live** (`write_diff_to_es=True`): ES receives only the diff.
- This project is a dependency for a Q4 initiative called "Rebuild From Self."

## 4. Write patterns and flags

Class-level attributes on `ESWriteJob` (set per job in the notebook tail, or defaulted):
`staging_schema="silver_creator_insights_staging"`, `replica_schema="silver_creator_insights_replicas"`,
`use_diff_upserts=False`, `write_diff_to_es=False`, `is_snapshot_job=False`.

| Pattern | Flags | ES on upsert | Replica on upsert |
|---|---|---|---|
| Plain (rolling rewrite) | both False | full window | (none) |
| Diff — shadow | `use_diff_upserts=True`, `write_diff_to_es=False` | full window | merge the diff |
| Diff — live | `use_diff_upserts=True`, `write_diff_to_es=True` | only the diff | merge the diff |
| Snapshot | `is_snapshot_job=True` | full write | merge the full snapshot |

- **Snapshot jobs** stamp `last_updated_at = current_timestamp()` on every row every run, so *every*
  row looks changed and a diff would be the entire table (useless). So the diff is skipped; ES gets
  the full write; the replica is updated. (See §9m for the overwrite→merge change.)
- On **rebuild**, the replica is seeded via a full overwrite (`overwrite_replica`) from the same data
  written to ES — the analog of the ES rebuild. Both diff and snapshot jobs seed on rebuild.

## 5. Key code locations

- **Framework:** `de_dbx/patreon_dbx/reverse_etl_new/jobs/elastic.py` (`ESWriteJob`). Methods:
  `rebuild()`, `upsert()`, `diff_based_write()`, `compute_diff()`, `write_to_staging()`,
  `merge_to_replica()`, `overwrite_replica()`, `_ensure_schema()`, `get_upsert_filter_time()`,
  `get_current_row_count()`.
- **Base class / dispatch:** `de_dbx/patreon_dbx/reverse_etl_new/etl.py` (`WriteJob`, `run_job`,
  `write_to_dbx`). Integration tests run in `write()`; UPSERT mode skips them; the
  `skip_integration_tests` Databricks widget gates the REBUILD-mode tests.
- **Sibling job (pattern reference):** `de_dbx/patreon_dbx/reverse_etl_new/jobs/delta.py`
  (`DeltaWriteJob`) — does `CREATE SCHEMA IF NOT EXISTS` before writing (the pattern `_ensure_schema`
  copied).
- **Monitoring:** `de_dbx/patreon_dbx/reverse_etl_new/monitoring.py` (`DatadogClient.metric` shells
  out to `dogshell`, absent on dev interactive clusters).
- **Job notebooks:** `data_jobs/src/creator_insights/jobs/*.py` (flags set at the notebook tail,
  e.g. `job.use_diff_upserts = True`).
- **table-api defs:** `table-api/src/table_api/datasets/silver_creator_insights_{staging,replicas}/*.py`.
  These register the physical tables for governance/GDPR; they do NOT create the tables (the
  framework's `saveAsTable` does).
- **Airflow:** `airflow/dags/creator_insights/configs/{rebuild,upsert}.yml` (which jobs run in each
  schedule) and DAG files `rebuild_jobs.py`, `one_hourly_upsert_jobs.py`,
  `half_hourly_upsert_jobs.py`, `four_hourly_upsert_jobs.py`. Rebuild DAGs are one-per-job; upsert
  DAGs are bundled by cadence.
- **Tests:** `de_dbx/tests/reverse_etl_new/jobs/test_elastic.py`.
- **GDPR nuke sources:** `data_infra.gdpr_nuker_campaign_ids` and `data_infra.gdpr_nuker_user_ids`
  (defs in `table-api/src/table_api/datasets/data_infra/gdpr_nuker_audit.py`); columns
  `entity_id` (string), `scheduled_at` (date, 7-day retention), `deletion_run_id`.

## 6. POC jobs (first three)

- `post_conversions_daily` — Diff. `campaign_id` is `IntegerType` (IntColumn).
- `creator_traffic_daily` — Diff. `campaign_id` is `LongType`. ~1.4B rows in ES.
- `active_free_trials_snapshot` — Snapshot. `campaign_id` is `LongType`. Its `id` concatenates
  campaign_id, tier_id, trial_length, and several state flags (status, is_cancelled, is_converted,
  and the *time-relative* `trial_started_last_7_days` / `trial_ending_next_7_days`), so the `id`
  changes as state or time passes — a big source of churn/stale rows.

## 7. Rollout plan (in Notion)

Two Notion pages: **"Rollout Plan"** and **"Creator Insights Jobs"** (the job inventory with
Scheduled Rebuild? / Upsert DAG? columns). Structure:

- **Prep:** split every bundled upsert DAG into per-job DAGs so any single job can be paused from
  the Airflow UI (~70 lines each). Decided: split ALL up front.
- **Waves (risk-ordered):** Wave 0 = POC; Wave 1 = snapshot jobs (safest, no ES cutover); Wave 2 =
  low-risk diff jobs; Wave 3 = payments/earnings/membership diff jobs (highest blast radius, one job
  per cutover PR).
- **Excluded:** rebuild-only jobs (`post_insights_hourly` is dead, `post_and_media_insights_daily`,
  `average_email_and_push_ctrs_retl`), rETLs (`*_retl` — the rETL framework has no upserts),
  `batched_post_impressions_daily` (upsert DAG but no rebuild DAG → can't seed the replica →
  deferred).
- **PRs:** framework PR #8165 (flags + logic, prerequisite, merged); DAG-split PRs; table-api def
  PRs; per-wave shadow-config PRs; per-job live-cutover PRs. Rule: no more than 4-5 jobs per PR
  (shadow and live) for easy revert.
- **Monitoring:** Airflow (task success/duration) + Databricks row-count reconciliation. No Datadog
  metrics for this project.
- **Rollback:** revert the PR that set `write_diff_to_es=True` (back to full rewrite), rerun rebuild
  + upsert. Replica/staging tables can stay.
- **Timeline risks:** intern summit week of ~July 20; several jobs have NO scheduled rebuild.

## 8. Job inventory notes

- **Scheduled-rebuild reality differs from `rebuild.yml`** (some DAGs are paused). Use the "Creator
  Insights Jobs" Notion page's "Scheduled Rebuild?" column as truth. Notably
  `post_conversions_daily`, `creator_traffic_daily`, and `creator_traffic_hourly` have **no
  scheduled rebuild**, so their replica isn't auto-reseeded and deleted data lingers in ES until a
  manual rebuild.
- Bundled upsert DAGs: `one_hourly` (3 jobs: creator_traffic_hourly, creator_traffic_daily,
  campaign_member_ltv), `four_hourly` (~20 jobs), `half_hourly` (only batched_post_impressions_daily).

## 9. Problems encountered and how they were resolved

a. **Rebuild didn't seed the replica** — `use_diff_upserts` was still False, so the seed gate was
   skipped. Fix: set `job.use_diff_upserts = True`.

b. **`dogshell` FileNotFoundError on dev** — the `index_lag_minutes` Datadog metric shells out to
   `dogshell`, absent on dev interactive clusters. It crashes before the write, so nothing is
   written. Workaround: monkeypatch `DatadogClient.metric = lambda self,*a,**k: None`, or run on a
   job cluster.

c. **Orphaned ES index** (`resource_already_exists_exception ...-002` from an earlier OOM'd
   rebuild) — the new version = current write-alias + 1 collided with a leftover. Fix: verify
   aliases point to the right index, delete the orphan, rerun.

d. **Snapshot diff = whole table** — `last_updated_at = current_timestamp()` makes every row differ
   each run. Fix: the `is_snapshot_job` flag (skip the diff; full write to ES; update replica).

e. **FMS integration test failure on the post_conversions rebuild** (`total_attributed_fms >
   total_fms`, `post_conversions_daily.py` ~line 641). It's the job's own pre-existing data-quality
   test, NOT caused by the diff work (the flags don't touch `generate_data`/`run_integration_tests`).
   Only REBUILD runs integration tests; **UPSERT skips them entirely** (etl.py ~line 534). Bypass:
   set the `skip_integration_tests` widget to `true` at run time (Zoe/creator-insights OK'd, since
   the job logic isn't changing). No file change needed — it's a widget.

f. **`SCHEMA_NOT_FOUND prod.silver_creator_insights_replicas`** — `saveAsTable`/`DeltaTable` create
   the *table* but not the *schema*. Dev had the schema (created earlier); prod never did. Fix
   (separate "schema-fix" PR): added `_ensure_schema()` (`CREATE SCHEMA IF NOT EXISTS`, dev/prod
   aware) called from `overwrite_replica`, `write_to_staging`, and the upsert guard — mirrors
   `DeltaWriteJob`/`write_to_dbx`. So the framework self-creates the schema; no manual prod DDL.
   Interim unblock: someone with prod access created the `silver_creator_insights_replicas` and
   `silver_creator_insights_staging` schemas in prod by hand.

g. **Fail-soft guard could crash on a missing schema** — the diff guard calls
   `spark.catalog.tableExists(replica_name)`, which can raise `SCHEMA_NOT_FOUND` when the schema is
   absent (so the "fail-soft" skip wasn't fail-soft). Fixed by ensuring the schema exists before the
   `tableExists` check (part of the schema-fix PR).

h. **Airflow rebuild DAG doesn't forward `dag_run.conf`** — so `skip_integration_tests` can't be
   passed via the Airflow "Trigger w/ config" UI (the DAG hardcodes `params={"mode":"rebuild"}` and
   only templates dates). To pass the widget, run the notebook directly in Databricks (Jobs UI "Run
   with different parameters", or the notebook widget), or change the DAG to merge conf into params.

i. **Replica-vs-ES row-count gaps** — methodology that worked: compare **by month**, then drill to
   **by day**; reconcile the by-date sum against the totals (docs with null/missing `date` won't
   appear in a date histogram but do count in the total); use `DESCRIBE HISTORY` `operationMetrics`
   (`numTargetRowsInserted/Updated/Deleted`, `numSourceRows`) to see what each merge did; and for
   small gaps, an id-level set diff. Two benign causes identified: (1) **freshest-day lag** — the
   replica trails ES by a hair on the current partial day and self-corrects next upsert; (2) **GDPR
   nuking** (below).

j. **GDPR nuking is deleting from the replica but not ES** — confirmed via `DESCRIBE HISTORY`: the
   deletes are MERGE ops with `matchedPredicates: delete` on `campaign_id = cast(entity_id as int)`,
   run by the compliance nuker (NOT the upsert; `merge_to_replica` never deletes). The replica tables
   were registered with `nukable_entity_type=CAMPAIGN`, enrolling them in the nuke. ES isn't nuked by
   that process (ES only clears on rebuild), so replica < ES, smeared across every month a nuked
   campaign had data. Exact reconciliations: post_conversions gap of 89 = 4 nuked campaigns' rows ES
   retained (replica had 0 of them). creator_traffic gap of 25,612 = 25,670 nuked rows in ES − 59
   still in the replica (58 re-added by the diff before the source-side nuke caught up) − 1
   freshest-day row. The nuked campaigns were verified as removed Patreon pages. Nuked campaign IDs
   come from `data_infra.gdpr_nuker_campaign_ids.entity_id` (NOT from the `operationParameters`
   predicate — those `#NNNN` numbers are Spark internal attribute IDs, not values).

k. **Spark attribute-id confusion** — `campaign_id#534089` in a plan/predicate is the column plus
   Spark's internal expression id `#534089`; it is not a campaign id.

l. **Snapshot replica: overwrite → merge** — see §9m / decision below.

m. **Snapshot replica behavior change (implemented, uncommitted at last check)** — snapshot upserts
   used to *overwrite* the replica (clean current snapshot). Decision (with mentor): the replica
   should mirror ES, so snapshot upserts now **merge** the current snapshot into the replica
   (update matched, insert new, never delete), with a fallback to overwrite/seed if the table
   doesn't exist yet. `merge_to_replica` was generalized (param `diff`→`rows`) to serve both diff
   and snapshot jobs. Rebuild still overwrites (fresh seed). Added tests
   `test_snapshot_upsert_merges_into_replica_when_it_exists` and `..._seeds_replica_when_missing`.

## 10. Decisions and people

- **Mahdir** — mentor (rollout guidance; framing: "researched, don't expect disruption, feedback
  welcome, rollback ready"; deletes via rebuild; Databricks as source of truth).
- **Patrick Song** — collaborator on the rollout plan.
- **Zoe** (creator insights) — OK'd bypassing integration tests since the job logic isn't changing.
- **Creator insights team** — rebuilds are safe to run manually; big jobs use rebuild + chunked
  backfill.
- Decisions: full phased migration; split all bundled DAGs up front;
  `batched_post_impressions_daily` out of v1; snapshot replica merges (mirror ES); temporarily
  EXEMPT campaign_id from nuking on the replica + staging defs so the replica mirrors ES.

## 11. GDPR nuking decision (current work)

Set `campaign_id` to `NukableEntityType.EXEMPT` (not deleted — an unmarked entity-ID column is a
review blocker) on all 5 POC defs (3 replicas + 2 staging: post_conversions, creator_traffic +
active_free_trials replica). Effect: the nuker stops deleting campaigns from the replica, so it
mirrors ES (which retains deleted campaigns until rebuild). Inline comments mark this as temporary.
**Stretch goal:** flip `EXEMPT` → `CAMPAIGN` and add matching ES-side deletion so both stay
compliant and in sync.

## 12. Current status (as of last session)

- Framework PR #8165 (flags + core logic): merged to main / in prod.
- Schema-fix PR (`_ensure_schema`): up for review.
- Snapshot overwrite→merge PR: up; added the two snapshot tests after a review comment asked about
  test coverage (existing upsert tests all run `is_snapshot_job=False`, so none needed updating).
- EXEMPT-nuking change: implemented in the 5 table-api defs, not committed.
- POC jobs: post_conversions + creator_traffic running in **shadow** (`write_diff_to_es=False`);
  active_free_trials on the snapshot path. Replica vs ES fully reconciled (gaps = nuke + freshest-day
  lag, both explained/benign). Not yet cut over to live.

## 13. Stretch goals / open items

- Re-enable `CAMPAIGN` nuking + add ES-side deletion (dual nuking).
- Data-validation / expectation tests on upserts (post-migration).
- Rebuild + chunked-backfill seeding for large jobs.
- A deletion path for ES on jobs with no scheduled rebuild (GDPR-deleted data lingers otherwise).
- Confirm which `*_rollup_snapshot` / `*_retl` jobs are true ES snapshot jobs; confirm the
  `*_combined_hourly` membership jobs aren't streaming (diff upserts are batch-only).
- `consumable_media_play_time_by_session` and `pending_earnings_snapshot_retl` classification.

## 14. Useful queries / commands

- Merge metrics per upsert: `SELECT version, operationMetrics['numTargetRowsInserted'] AS inserted,
  operationMetrics['numTargetRowsUpdated'] AS updated, operationMetrics['numTargetRowsDeleted'] AS
  deleted, operationMetrics['numSourceRows'] AS diff_rows FROM (DESCRIBE HISTORY
  prod.<schema>.<table>) WHERE operation='MERGE' ORDER BY version DESC`.
- Nuked campaign IDs: `SELECT DISTINCT entity_id AS campaign_id, scheduled_at, deletion_run_id FROM
  prod.data_infra.gdpr_nuker_campaign_ids WHERE scheduled_at >= '<date>'`.
- Replica by-month: `SELECT date_format(date,'yyyy-MM') month, count(*) FROM prod.<replica> GROUP BY
  1 ORDER BY 1`. ES equivalent: Kibana `date_histogram` on `date` (`calendar_interval:"month"`,
  `min_doc_count:1`).
- ES row count: `job.get_current_row_count()` or Kibana `_count` / Discover.
- Bypass integration tests on a rebuild: run the notebook with the `skip_integration_tests` widget
  set to `true` (REBUILD only; upserts already skip).

## 15. Environment / tooling gotchas

- **Prod Databricks is read-only** for agents (AGENTS.md). Writes target `dev.` or rely on
  `get_workspace()` routing. The prod catalog is mounted read-only in the dev workspace, so
  `prod.*` Delta tables are queryable from dev.
- **The dev workspace cannot reach the prod ES cluster.** Prod ES is read via **Kibana Dev Tools**
  (`GET <index>-search/_search { ... }`). `job.es_client` points at prod ES only when the job runs
  in the prod workspace.
- **No Datadog** for this project; monitor via Airflow + row counts.
- Default catalog in prod is `prod`; dev tables are prefixed `dev.` in the framework.
- **Standing constraint:** do not commit — the user commits themselves. External messages: plain,
  contractions, no em dashes, not AI-sounding, concise.

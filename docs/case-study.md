# Cadence: a scheduled pipeline that runs itself and recovers when it breaks

**Stack:** Apache Airflow, dbt, DuckDB, Python, Docker, GitHub Actions

**Companion to:** [Pulse](https://github.com/Mprtham/pulse)

---

## The plain version

Pulse is a machine that processes data. Cadence is the manager standing over that machine. It decides when the machine runs, checks each step finished before starting the next, tries again if a step fails, and raises an alarm if something stays broken. It can also re-run any past day on demand without double-counting. That pattern, a scheduled job with retries and alerting over a multi-step process, is what most UK data-engineering job descriptions mean when they list Airflow.

---

## What I built

One Airflow DAG called `retail_daily` on an hourly schedule. Four tasks in strict order:

**Generate.** Produces a batch of synthetic UK-retail orders for the run date. The schema follows UCI Online Retail II: invoice number, stock code, description, quantity, date, unit price, customer ID, country. About 15% of rows are intentionally faulty: negative quantities (returns), missing customer IDs, zero or negative prices, duplicate lines. These faults are what the pipeline exists to find and filter.

**Load.** Reads the generated CSV and writes it into a DuckDB table called `raw_orders`. Before inserting, it deletes any existing rows for that date. That delete-then-insert pattern is what makes the load idempotent: re-running the same date replaces the rows, it does not add to them. Backfills are safe because of this.

**dbt run.** Runs the dbt models. The staging model reads from `raw_orders` and filters out the faults: it keeps only rows with positive quantity, positive price, and a non-null customer ID. The mart models aggregate the clean rows into daily revenue totals and revenue by country.

**dbt test.** Runs schema tests on the output: not-null checks, uniqueness checks, and accepted-values checks on country names. If the tests fail, the task fails. Downstream consumers never see bad data because the pipeline stops before writing it.

Every task has `retries=2` with exponential backoff. If a task fails twice, a callback fires a Discord message with the task name, run date, and a link to the logs.

---

## The problems this solves

**Manual runs break at the worst times.** A scheduled DAG runs on its own. No one has to remember to run it, and no run is missed because it was a holiday or a Friday afternoon.

**One failure should not silently skip downstream steps.** Airflow's dependency graph means `dbt_run` never starts until `load_to_duckdb` succeeds, and `dbt_test` never starts until `dbt_run` succeeds. The graph makes the dependency explicit and enforced, not just a comment in a script.

**Transient failures should not page anyone.** Most failures in a real pipeline (a network blip, a file not yet available) resolve on the next attempt. Exponential backoff gives the system time to recover without human intervention. The alert fires only after both retries fail, which means it represents a genuine problem.

**Backfills should not corrupt data.** When you need to re-process a past date (because logic changed, or because a run failed), loading the same date twice must not double the row counts. The delete-then-insert pattern in `load.py` guarantees it. I proved this by running a three-day backfill twice and confirming the row counts did not change on the second run.

**Bad data should fail loudly, not pass silently.** The dbt test task is the gate. If data quality drops below the schema tests, the task fails, the alert fires, and nothing downstream gets written. This is the difference between a pipeline that delivers bad numbers quietly and one that stops and tells you.

---

## What I learnt

**Idempotency is a design decision, not a default.** Writing a loader that is safe to re-run requires a deliberate choice about the key. Here the key is the run date. In a production system it might be a batch ID or a watermark. The principle is the same: identify what makes a batch unique, delete it before re-inserting, never assume the table was empty.

**Retries need a strategy, not just a number.** Setting `retry_exponential_backoff=True` with a `max_retry_delay` means the second retry waits longer than the first, which gives transient failures more time to clear. Without the maximum, exponential backoff can produce waits long enough to miss the next scheduled run.

**The test task is not optional.** The temptation in a weekend project is to skip the test gate and call it done after `dbt_run`. That misses the point. The gate is what turns a script into a pipeline: the pipeline knows whether it succeeded, and it stops when it does not.

**Airflow standalone is a genuine alternative for development.** Docker is the realistic production-like setup and the better story for a portfolio, but `airflow standalone` runs the full scheduler and webserver with a single command and no containers. I built and tested the DAG logic locally on standalone, then verified it worked identically under Docker. The DAG file has no dependency on the hosting method.

---

## Evidence

- DAG-graph screenshot: all four tasks green after a triggered run. See `docs/dag-graph.png`.
- Grid view: a three-day backfill, all runs green. See `docs/backfill-grid.png`.
- GitHub Actions: the CI job imports the DAG and asserts task order on every push.

---

## Interview lines

"I built an hourly Airflow DAG orchestrating extract, load, transform and test over a synthetic UK-retail dataset."

"Each task retries with exponential backoff. A failure after both retries posts a Discord alert with the task name and run date."

"The load is idempotent on the run date, so backfills and re-runs never double-count."

"dbt test is a gating task in the DAG. If data quality drops, the pipeline fails loudly before anything downstream sees the data."

---

*Prathamesh Mishra, June 2025. Synthetic data, clearly labelled.*

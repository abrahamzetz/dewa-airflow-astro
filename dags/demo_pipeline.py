"""
Intro to Airflow — Student Lab DAG
===================================
A fake ELT pipeline. No real API, no real Snowflake, no real dbt.
The point is to understand Airflow mechanics: task states, dependencies, logs.

YOUR TASKS:
  1. Run the DAG and watch all tasks go green in the UI
  2. Click into each task and read the logs — find the output lines
  3. Make one change (see the challenges at the bottom) and re-run
  4. Uncomment the validate() task AND its dependency line to see what a failure looks like
"""

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


@dag(
    dag_id="demo_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lab", "intro"],
)
def demo_pipeline():

    @task
    def extract() -> int:
        """Pretend to fetch data from an API."""
        print("Connecting to Fingrid API...")
        print("Fetching electricity consumption data...")
        row_count = 500
        print(f"Done. Fetched {row_count} rows.")
        return row_count

    @task
    def load(row_count: int) -> None:
        """Pretend to load raw data into Snowflake."""
        print("Connecting to Snowflake...")
        print(f"Loading {row_count} raw rows into RAW.fingrid.consumption...")
        print("Load complete.")

    transform = BashOperator(
        task_id="transform",
        bash_command=(
            'echo "Running dbt models..." && '
            'echo "  -> model: stg_fingrid__consumption ... OK" && '
            'echo "  -> model: int_consumption_daily    ... OK" && '
            'echo "  -> model: mart_consumption_summary ... OK" && '
            'echo "All models finished successfully."'
        ),
    )

    # ── Uncomment this task for Challenge 4 ──────────────────────────
    # @task
    # def validate() -> None:
    #     """This task will fail on purpose. Read the logs to find out why."""
    #     print("Running data quality checks...")
    #     row_count = 0  # simulating an empty load
    #     if row_count == 0:
    #         raise ValueError("Validation failed: no rows were loaded. Check the extract task.")
    # ─────────────────────────────────────────────────────────────────

    # Dependencies: extract first, then load, then transform
    # TaskFlow tasks chain via function calls; BashOperator chains via >>
    extract_task = extract()
    load_task = load(extract_task)

    # Note: load(extract_task) above already creates extract -> load.
    # We spell the full chain out with >> anyway so the whole pipeline
    # is visible on one line.
    extract_task >> load_task >> transform

    # ── Uncomment this line if you uncommented validate() above ──────
    # transform >> validate()
    # ─────────────────────────────────────────────────────────────────


demo_pipeline()


# =============================================================================
# CHALLENGES
# =============================================================================
#
# 1. RUN IT
#    Trigger the DAG manually in the UI. Watch the graph view.
#    When it's done, click each task and open the logs.
#    Find the output lines (print from @task, echo from BashOperator).
#
# 2. CHANGE THE ROW COUNT
#    In the extract() task, change row_count from 500 to 9999.
#    Save the file and re-trigger the DAG.
#    Does the change show up in the load() task logs?
#
# 3. ADD A FOURTH TASK
#    Add a new notify task that outputs:
#    "Pipeline complete. Sending summary email..."
#    You can use either a @task (with print) or a BashOperator (with echo).
#    Make it run after transform using >>.
#
# 4. BREAK IT ON PURPOSE
#    Uncomment the validate() task and its dependency above.
#    Re-trigger the DAG. Which task goes red?
#    Open the logs and find the error message.
#
# 5. CHANGE THE SCHEDULE (bonus)
#    Change schedule="@daily" to schedule="0 6 * * 1"
#    What does that mean? (hint: crontab.guru)
# =============================================================================

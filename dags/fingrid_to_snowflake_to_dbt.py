"""
Fingrid -> Snowflake -> dbt demo DAG.

Daily pipeline: extract from Fingrid (with publish-lag offset),
load into Snowflake, clone the latest dbt project, then run dbt.
"""

import datetime
import os

import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include import fingrid_extract

# Set DBT_REPO_URL in .env to point at your own dbt project on GitHub.
DBT_REPO_URL = os.environ["DBT_REPO_URL"]
DBT_CHECKOUT_DIR = "/tmp/dbt"
DBT_PROFILES_DIR = "/usr/local/airflow/include/dbt-profiles"
SNOWFLAKE_CONN_ID = "snowflake_default"

# Fingrid publishes with a delay - fetch data from this many days ago.
FINGRID_PUBLISH_LAG_DAYS = 7


@dag(
    dag_id="fingrid_to_snowflake_to_dbt",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["demo", "fingrid", "snowflake", "dbt"],
)
def fingrid_pipeline():

    @task
    def extract_from_fingrid(data_interval_start=None):
        # data_interval_start is auto-injected by Airflow - it's the start
        # of the time window this DAG run covers.
        dag_run_date = data_interval_start.date()
        fingrid_data_date = dag_run_date - datetime.timedelta(days=FINGRID_PUBLISH_LAG_DAYS)
        print(f"DAG run date: {dag_run_date} | Fetching Fingrid data for: {fingrid_data_date}")
        return fingrid_extract.fetch_consumption(fingrid_data_date, os.environ["FINGRID_API_KEY"])

    @task
    def load_to_snowflake(rows, data_interval_start=None):
        # SnowflakeHook uses the `snowflake_default` connection from airflow_settings.yaml
        # so credentials stay out of the DAG. It also handles connect/insert/commit/close.
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        # Skip if we already have rows for this date - keeps re-runs idempotent.
        dag_run_date = data_interval_start.date()
        fingrid_data_date = dag_run_date - datetime.timedelta(days=FINGRID_PUBLISH_LAG_DAYS)
        existing = hook.get_first(
            f"SELECT count(*) FROM raw.fingrid.consumption "
            f"WHERE date(start_time) = '{fingrid_data_date}'"
        )[0]
        if existing > 0:
            print(f"Found {existing} existing rows for {fingrid_data_date}, skipping load")
            return

        hook.insert_rows(
            table="raw.fingrid.consumption",
            rows=rows,
            target_fields=("dataset_id", "start_time", "end_time", "value"),
        )
        print(f"Loaded {len(rows)} rows into raw.fingrid.consumption for {fingrid_data_date}")

    # Fresh shallow clone of main, then dbt run. Combined into one task so
    # the cloned directory doesn't have to survive across task boundaries.
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"rm -rf {DBT_CHECKOUT_DIR} && "
            f"git clone --depth 1 {DBT_REPO_URL} {DBT_CHECKOUT_DIR} && "
            f"cd {DBT_CHECKOUT_DIR} && "
            f"dbt deps && "  # install dbt deps before the run
            f"dbt run --select tag:fingrid --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # Build tasks. load_to_snowflake(extract_task) passes extract's rows via XCom.
    extract_task = extract_from_fingrid()
    load_task = load_to_snowflake(extract_task)

    # Chain order
    extract_task >> load_task >> dbt_run


fingrid_pipeline()

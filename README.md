# Intro to Airflow — demo project

Astro CLI project for the closing demo of the Airflow session.

The DAG `fingrid_to_snowflake_to_dbt`:

1. **Extract** — pull a day of electricity consumption from the Fingrid API.
2. **Load** — write rows into `raw.fingrid.consumption` in Snowflake.
3. **Git clone dbt** — shallow-clone the latest `main` of [dewa-dbt-demo](https://github.com/abrahamzetz/dewa-dbt-demo) into `/tmp/dbt`.
4. **dbt run** — run dbt against the freshly cloned project.

Cloning fresh on every run means whatever's on `main` is what gets executed — no state drift between a local dbt copy and Airflow.

It's a demo, not a lab — students watch you trigger it and walk through the UI.

---

## One-time setup

### 1. Install Astro CLI

```sh
brew install astro
```

### 2. Create the Snowflake target

Run `include/snowflake_setup.sql` once in a Snowflake worksheet. It creates `raw.fingrid` schema and the `consumption` table.

### 3. Fill in secrets

Three files, all gitignored. Don't open them during the demo, and rotate the Snowflake password after the session as a final safety net.

**`.env`** — Fingrid API key (read in the DAG via `os.environ["FINGRID_API_KEY"]`):

```sh
FINGRID_API_KEY=<your-fingrid-key>
```

**`airflow_settings.yaml`** — Snowflake connection used by the Python load task. Replace the placeholders with real values. Astro loads this at startup and registers the connection as `snowflake_default`, which the DAG's `SnowflakeHook` picks up by conn_id.

**`include/dbt-profiles/profiles.yml`** — Snowflake connection used by dbt. Replace the placeholders with real values. The DAG's `dbt_run` task points `--profiles-dir` at this folder.

### 4. (Optional) Local dbt working copy

The DAG clones `dewa-dbt-demo` from GitHub on every run, so you don't need a local copy in this project. If you're developing the dbt project, keep your working copy anywhere outside `airflow/` (it's its own git repo), push to `main`, and the next DAG run picks it up.

---

## Running the demo

```sh
astro dev start
```

Then `http://localhost:8080` (login `admin` / `admin`):

1. Unpause `fingrid_to_snowflake_to_dbt`.
2. Trigger a run manually.
3. Switch to Graph view — watch tasks turn green: `extract_from_fingrid → load_to_snowflake → git_clone_dbt → dbt_run`.
4. Click into `extract_from_fingrid` / `load_to_snowflake` logs — show the row counts.
5. Click into `git_clone_dbt` logs — show the clone output (good teaching moment: "every run gets the latest code from main").
6. Click into `dbt_run` logs — show dbt output.
7. Back in Snowflake: `select count(*) from raw.fingrid.consumption;`

To stop:

```sh
astro dev stop
```

---

## Talking points during the demo

- "This DAG is exactly what we drew on the whiteboard: extract, load, transform."
- Point at `extract_from_fingrid` — "this is the Python you saw in the batch session, wrapped as an Airflow task."
- Point at `dbt_run` — "this is just a BashOperator calling `dbt run` — Airflow doesn't care that it's dbt."
- Show the schedule (`@daily`) — "this would run by itself every night without us touching it."
- Show a failed run if you have time — break the API key, re-trigger, walk through the log.

---

## Folder layout

```
airflow/
├── Dockerfile                        # Astro Runtime base image
├── packages.txt                      # OS packages (git, for the clone task)
├── requirements.txt                  # Python deps (Snowflake provider, dbt, requests)
├── airflow_settings.yaml             # Snowflake conn for the load task (gitignored)
├── .env                              # FINGRID_API_KEY (gitignored)
├── .dockerignore
├── .gitignore
├── README.md
├── dags/
│   └── fingrid_to_snowflake_to_dbt.py
└── include/
    ├── __init__.py
    ├── fingrid_extract.py
    ├── snowflake_setup.sql
    └── dbt-profiles/
        └── profiles.yml              # Snowflake conn for dbt (gitignored)
```

The dbt project itself isn't stored in this repo — `git_clone_dbt` fetches the latest `main` of [dewa-dbt-demo](https://github.com/abrahamzetz/dewa-dbt-demo) into `/tmp/dbt` inside the container on every DAG run.

# Intro to Airflow — demo project

Astro CLI project for the closing demo of the Airflow session.

The DAG `fingrid_to_snowflake_to_dbt`:

1. **Extract** — pull a day of electricity consumption from the Fingrid API.
2. **Load** — write rows into `raw.fingrid.consumption` in Snowflake.
3. **dbt run** — shallow-clone the latest `main` of [dewa-dbt-demo](https://github.com/abrahamzetz/dewa-dbt-demo) into `/tmp/dbt`, then run dbt against it.

Cloning fresh on every run means whatever's on `main` is what gets executed — no state drift between a local dbt copy and Airflow.

It's a demo, not a lab — students watch you trigger it and walk through the UI.

---

## One-time setup

### 1. Install Astro CLI

Astro CLI docs: https://www.astronomer.io/docs/astro/cli/install-cli

If you're on macOS with [Homebrew](https://brew.sh) installed:

```sh
brew install astro
```

Otherwise follow the install instructions in the Astro docs above for your platform.

### 2. Create the Snowflake target

Run `include/snowflake_setup.sql` once in a Snowflake worksheet. It creates `raw.fingrid` schema and the `consumption` table.

### 3. Create credential files

All three credential files are gitignored, so after cloning this repo you need to create them yourself. Rotate the Snowflake password after the demo as a final safety net.

#### `.env`

Create `.env` in the project root:

```sh
FINGRID_API_KEY=<your-fingrid-key>
```

Astro loads this into the running containers. The DAG reads it via `os.environ["FINGRID_API_KEY"]`.

#### `airflow_settings.yaml`

Create `airflow_settings.yaml` in the project root. This registers the Snowflake connection used by the Python `load_to_snowflake` task:

```yaml
airflow:
  connections:
    - conn_id: snowflake_default
      conn_type: snowflake
      conn_login: <your-snowflake-user>
      conn_password: <your-snowflake-password>
      conn_schema: fingrid
      conn_extra:
        account: <your-snowflake-account>
        warehouse: transforming
        database: raw
        role: sysadmin
  pools: []
  variables: []
```

Astro loads this at startup. The DAG's `SnowflakeHook` picks it up by `conn_id="snowflake_default"`.

#### `include/dbt-profiles/profiles.yml`

Create `include/dbt-profiles/profiles.yml`. This is the connection dbt uses:

```yaml
default:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your-snowflake-account>
      user: <your-snowflake-user>
      password: <your-snowflake-password>
      role: sysadmin
      warehouse: transforming
      database: analytics
      schema: dbt_demo
      threads: 4
```

The top-level key (`default`) must match `profile:` in the `dbt_project.yml` of [dewa-dbt-demo](https://github.com/abrahamzetz/dewa-dbt-demo). The DAG's `dbt_run` task points `--profiles-dir` at this folder.

### 4. (Optional) Local dbt working copy

The DAG clones `dewa-dbt-demo` from GitHub on every run, so you don't need a local copy in this project. If you're developing the dbt project, keep your working copy anywhere outside `airflow-astro/` (it's its own git repo), push to `main`, and the next DAG run picks it up.

---

## Running the demo

```sh
astro dev start
```

Then `http://localhost:8080` (login `admin` / `admin`):

1. Unpause `fingrid_to_snowflake_to_dbt`.
2. Trigger a run manually.
3. Switch to Graph view — watch tasks turn green: `extract_from_fingrid → load_to_snowflake → dbt_run`.
4. Click into `extract_from_fingrid` / `load_to_snowflake` logs — show the row counts and date being processed.
5. Click into `dbt_run` logs — show the git clone output followed by dbt output. Good teaching moment: "every run gets the latest code from main and then transforms."
6. Back in Snowflake: `select count(*) from raw.fingrid.consumption;`

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
airflow-astro/
├── Dockerfile                        # Astro Runtime base image
├── packages.txt                      # OS packages (git, for the clone in dbt_run)
├── requirements.txt                  # Python deps (Snowflake provider, dbt)
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

The dbt project itself isn't stored in this repo — `dbt_run` fetches the latest `main` of [dewa-dbt-demo](https://github.com/abrahamzetz/dewa-dbt-demo) into `/tmp/dbt` inside the container on every DAG run.

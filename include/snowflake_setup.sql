-- Run once in Snowflake before triggering the DAG.

use role sysadmin;

create schema if not exists raw.fingrid;

create table if not exists raw.fingrid.consumption (
    dataset_id  number,
    start_time  timestamp_tz,
    end_time    timestamp_tz,
    value       float,
    _loaded_at  timestamp_ntz default current_timestamp
);

-- Sanity check after a DAG run:
-- select count(*), max(_loaded_at) from raw.fingrid.consumption;

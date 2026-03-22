import datetime

import dagster as dg

import polars as pl


@dg.asset(io_manager_key="excellake")
def example__fake_users():
    fake_users = pl.from_dicts(
        [
            {
                "firstname": "john",
                "lastname": "smith",
                "date_of_birth": datetime.date(1993, 1, 1),
            },
            {
                "firstname": "jane",
                "lastname": "smith",
                "date_of_birth": datetime.date(2000, 2, 1),
            },
        ]
    )
    return fake_users


@dg.asset(io_manager_key="excellake")
def example__fake_users_enhanced(example__fake_users: pl.DataFrame):
    return example__fake_users.with_columns(
        (pl.col("firstname") + pl.lit(" ") + pl.col("lastname")).alias("full name"),
        (
            (
                pl.lit(datetime.datetime.today()) - pl.col("date_of_birth")
            ).dt.total_days()
            / 365.25
        )
        .floor()
        .cast(pl.Int8)
        .alias("age"),
    )


@dg.asset(io_manager_key="excellake")
def example__fake_users_enhanced(example__fake_users: pl.DataFrame):
    return example__fake_users.with_columns(
        (pl.col("firstname") + pl.lit(" ") + pl.col("lastname")).alias("full name"),
    )


monthly_partition = dg.MonthlyPartitionsDefinition(start_date="2026-01-01")


@dg.asset(io_manager_key="excellake", partitions_def=monthly_partition)
def example__partitioned(context: dg.AssetExecutionContext):
    date = context.partition_key
    return pl.DataFrame(
        {
            "date": [date] * 10,
            "sales": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        }
    )

"""Gold-layer dimensional model definitions."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ---------------------------------------------------------------------
# Staging views
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------


@dp.table(
    name="dim_date",
    comment="Role-playing date dimension for Internet Sales order, due, and ship dates.",
    table_properties={
        "clusterBy": "auto",
    },
)
def dim_date():
    source_dates = spark.read.table("silver_sales_order_header").select(
        F.to_date("order_date").alias("order_date"),
        F.to_date("due_date").alias("due_date"),
        F.to_date("ship_date").alias("ship_date"),
    )

    date_bounds = source_dates.select(
        F.least(
            F.min("order_date"),
            F.min("due_date"),
            F.min("ship_date"),
        ).alias("min_date"),
        F.greatest(
            F.max("order_date"),
            F.max("due_date"),
            F.max("ship_date"),
        ).alias("max_date"),
    )

    dates = date_bounds.select(
        F.explode(
            F.sequence(
                F.date_sub("min_date", 365),
                F.date_add("max_date", 365),
                F.expr("INTERVAL 1 DAY"),
            )
        ).alias("date")
    )

    return dates.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        F.weekofyear("date").alias("week_of_year"),
    )


# ---------------------------------------------------------------------
# Inferred members
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------

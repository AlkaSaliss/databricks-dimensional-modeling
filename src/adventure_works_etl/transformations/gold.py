"""Gold-layer dimensional model definitions."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.helpers import record_hash


# ---------------------------------------------------------------------
# Staging views
# ---------------------------------------------------------------------


@dp.view(
    name="stg_dim_customer",
    comment="Customer dimension staging view from customer, person, and email silver records.",
)
def stg_dim_customer():
    customer = spark.read.table("silver_sales_customer").alias("customer")
    person = spark.read.table("silver_person").alias("person")
    email = (
        spark.read.table("silver_person_email_address")
        .groupBy("business_entity_id")
        .agg(
            F.max_by("email_address", "email_address_id").alias("email_address"),
            F.max("modified_at").alias("email_modified_at"),
        )
        .alias("email")
    )

    return (
        customer.join(person, F.col("customer.person_id") == F.col("person.business_entity_id"), "left")
        .join(email, F.col("customer.person_id") == F.col("email.business_entity_id"), "left")
        .select(
            F.col("customer.customer_id"),
            F.col("customer.person_id"),
            F.col("customer.store_id"),
            F.col("customer.territory_id"),
            F.col("customer.account_number"),
            F.col("person.person_type"),
            F.col("person.title"),
            F.col("person.first_name"),
            F.col("person.middle_name"),
            F.col("person.last_name"),
            F.col("person.suffix"),
            F.col("person.email_promotion"),
            F.col("email.email_address"),
            F.col("person.birth_date"),
            F.col("person.marital_status"),
            F.col("person.yearly_income"),
            F.col("person.gender"),
            F.col("person.total_children"),
            F.col("person.number_children_at_home"),
            F.col("person.education"),
            F.col("person.occupation"),
            F.col("person.home_owner_flag"),
            F.col("person.number_cars_owned"),
            F.col("person.commute_distance"),
            F.greatest(
                F.col("customer.modified_at"),
                F.col("person.modified_at"),
                F.col("email.email_modified_at"),
            ).alias("modified_at"),
            F.col("customer.__source_file_name"),
            F.col("customer.__ingestion_time"),
            F.current_timestamp().alias("__processing_time"),
        )
    )


@dp.view(
    name="stg_dim_product",
    comment="Product dimension staging view from product, subcategory, and category silver records.",
)
def stg_dim_product():
    product = spark.read.table("silver_product").alias("product")
    subcategory = spark.read.table("silver_product_subcategory").alias("subcategory")
    category = spark.read.table("silver_product_category").alias("category")
    modified_at = F.greatest(
        F.col("product.modified_at"),
        F.col("subcategory.modified_at"),
        F.col("category.modified_at"),
    )

    return (
        product.join(
            subcategory,
            F.col("product.product_subcategory_id") == F.col("subcategory.product_subcategory_id"),
            "left",
        )
        .join(
            category,
            F.col("subcategory.product_category_id") == F.col("category.product_category_id"),
            "left",
        )
        .select(
            F.col("product.product_id"),
            record_hash(F.col("product.product_id"), modified_at, namespace="dim_product").alias("product_key"),
            F.col("product.product_number"),
            F.col("product.product_name"),
            F.col("product.make_flag"),
            F.col("product.finished_goods_flag"),
            F.col("product.color"),
            F.col("product.safety_stock_level"),
            F.col("product.reorder_point"),
            F.col("product.standard_cost"),
            F.col("product.list_price"),
            F.col("product.size"),
            F.col("product.size_unit_measure_code"),
            F.col("product.weight_unit_measure_code"),
            F.col("product.weight"),
            F.col("product.days_to_manufacture"),
            F.col("product.product_line"),
            F.col("product.class"),
            F.col("product.style"),
            F.col("product.product_subcategory_id"),
            F.col("subcategory.product_subcategory_name"),
            F.col("subcategory.product_category_id"),
            F.col("category.product_category_name"),
            F.col("product.product_model_id"),
            F.col("product.sell_start_date"),
            F.col("product.sell_end_date"),
            modified_at.alias("modified_at"),
            F.col("product.__source_file_name"),
            F.col("product.__ingestion_time"),
            F.current_timestamp().alias("__processing_time"),
        )
    )


@dp.view(
    name="stg_dim_promotion",
    comment="Promotion dimension staging view from special offer silver records.",
)
def stg_dim_promotion():
    return spark.read.table("silver_sales_special_offer").select(
        F.col("special_offer_id"),
        record_hash("special_offer_id", namespace="dim_promotion").alias("promotion_key"),
        F.col("description"),
        F.col("discount_pct"),
        F.col("promotion_type"),
        F.col("promotion_category"),
        F.col("start_date"),
        F.col("end_date"),
        F.col("min_qty"),
        F.col("max_qty"),
        F.col("modified_at"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.view(
    name="stg_dim_currency",
    comment="Currency dimension staging view from currency rate silver records.",
)
def stg_dim_currency():
    return spark.read.table("silver_sales_currency_rate").select(
        F.col("currency_rate_id"),
        record_hash(F.col("currency_rate_id"), F.col("modified_at"), namespace="dim_currency").alias("currency_key"),
        F.col("currency_rate_date"),
        F.col("from_currency_code"),
        F.col("to_currency_code"),
        F.col("average_rate"),
        F.col("end_of_day_rate"),
        F.col("modified_at"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.view(
    name="stg_dim_sales_territory",
    comment="Sales territory dimension staging view from sales territory silver records.",
)
def stg_dim_sales_territory():
    return spark.read.table("silver_sales_territory").select(
        F.col("territory_id"),
        record_hash(F.col("territory_id"), F.col("modified_at"), namespace="dim_sales_territory").alias(
            "sales_territory_key"
        ),
        F.col("territory_name"),
        F.col("country_region_code"),
        F.col("territory_group"),
        F.col("sales_ytd"),
        F.col("sales_last_year"),
        F.col("cost_ytd"),
        F.col("cost_last_year"),
        F.col("modified_at"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


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


dp.create_streaming_table(
    name="dim_promotion",
    comment="Type 1 promotion dimension maintained from promotion snapshots.",
    table_properties={
        "clusterBy": "auto",
    },
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_promotion",
    source="stg_dim_promotion",
    keys=["special_offer_id"],
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name="dim_product",
    comment="Type 2 product dimension maintained from product snapshots.",
    table_properties={
        "clusterBy": "auto",
    },
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_product",
    source="stg_dim_product",
    keys=["product_id"],
    stored_as_scd_type=2,
    track_history_except_column_list=[
        "product_key",
        "__source_file_name",
        "__ingestion_time",
        "__processing_time",
    ],
)

dp.create_streaming_table(
    name="dim_currency",
    comment="Type 2 currency rate dimension maintained from currency rate snapshots.",
    table_properties={
        "clusterBy": "auto",
    },
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_currency",
    source="stg_dim_currency",
    keys=["currency_rate_id"],
    stored_as_scd_type=2,
    track_history_except_column_list=[
        "currency_key",
        "__source_file_name",
        "__ingestion_time",
        "__processing_time",
    ],
)

dp.create_streaming_table(
    name="dim_sales_territory",
    comment="Type 2 sales territory dimension maintained from sales territory snapshots.",
    table_properties={
        "clusterBy": "auto",
    },
)

dp.create_auto_cdc_from_snapshot_flow(
    target="dim_sales_territory",
    source="stg_dim_sales_territory",
    keys=["territory_id"],
    stored_as_scd_type=2,
    track_history_except_column_list=[
        "sales_territory_key",
        "__source_file_name",
        "__ingestion_time",
        "__processing_time",
    ],
)


# ---------------------------------------------------------------------
# Inferred members
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------

"""BI-facing tables built from the gold dimensional model."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def null_safe_ratio(numerator, denominator):
    return F.when(denominator != F.lit(0), numerator / denominator)


@dp.table(
    name="bi_sales_order_line",
    comment="BI detail table for Internet Sales at sales order line grain.",
    table_properties={
        "clusterBy": "auto",
    },
)
def bi_sales_order_line():
    fact = spark.read.table("fact_internet_sales").alias("fact")
    product = spark.read.table("dim_product").alias("product")
    customer = spark.read.table("dim_customer").alias("customer")
    promotion = spark.read.table("dim_promotion").alias("promotion")
    currency = spark.read.table("dim_currency").alias("currency")
    territory = spark.read.table("dim_sales_territory").alias("territory")
    order_date = spark.read.table("dim_date").alias("order_date")
    due_date = spark.read.table("dim_date").alias("due_date")
    ship_date = spark.read.table("dim_date").alias("ship_date")

    gross_sales_amount = F.col("fact.sales_amount_before_discount")
    discount_amount = F.col("fact.discount_amount")
    customer_name = F.trim(
        F.concat_ws(
            " ",
            F.col("customer.first_name"),
            F.col("customer.middle_name"),
            F.col("customer.last_name"),
        )
    )

    return (
        fact.join(product, F.col("fact.product_key") == F.col("product.product_key"), "left")
        .join(customer, F.col("fact.customer_key") == F.col("customer.customer_key"), "left")
        .join(promotion, F.col("fact.promotion_key") == F.col("promotion.promotion_key"), "left")
        .join(currency, F.col("fact.currency_key") == F.col("currency.currency_key"), "left")
        .join(territory, F.col("fact.sales_territory_key") == F.col("territory.sales_territory_key"), "left")
        .join(order_date, F.col("fact.order_date_key") == F.col("order_date.date_key"), "left")
        .join(due_date, F.col("fact.due_date_key") == F.col("due_date.date_key"), "left")
        .join(ship_date, F.col("fact.ship_date_key") == F.col("ship_date.date_key"), "left")
        .select(
            F.col("fact.sales_order_id"),
            F.col("fact.sales_order_detail_id"),
            F.col("fact.sales_order_number"),
            F.col("fact.product_key"),
            F.col("product.product_id"),
            F.coalesce(F.col("product.product_name"), F.lit("Unknown")).alias("product_name"),
            F.coalesce(F.col("product.product_category_name"), F.lit("Unknown")).alias("product_category_name"),
            F.coalesce(F.col("product.product_subcategory_name"), F.lit("Unknown")).alias("product_subcategory_name"),
            F.coalesce(F.col("product.product_line"), F.lit("Unknown")).alias("product_line"),
            F.coalesce(F.col("product.class"), F.lit("Unknown")).alias("product_class"),
            F.coalesce(F.col("product.style"), F.lit("Unknown")).alias("product_style"),
            F.coalesce(F.col("product.color"), F.lit("Unknown")).alias("product_color"),
            F.col("fact.customer_key"),
            F.col("customer.customer_id"),
            F.when(customer_name != F.lit(""), customer_name).otherwise(F.lit("Unknown")).alias("customer_name"),
            F.coalesce(F.col("customer.yearly_income"), F.lit("Unknown")).alias("yearly_income"),
            F.coalesce(F.col("customer.education"), F.lit("Unknown")).alias("education"),
            F.coalesce(F.col("customer.occupation"), F.lit("Unknown")).alias("occupation"),
            F.coalesce(F.col("customer.gender"), F.lit("Unknown")).alias("gender"),
            F.coalesce(F.col("customer.marital_status"), F.lit("Unknown")).alias("marital_status"),
            F.coalesce(F.col("customer.home_owner_flag").cast("string"), F.lit("Unknown")).alias("home_owner_flag"),
            F.coalesce(F.col("customer.commute_distance"), F.lit("Unknown")).alias("commute_distance"),
            F.coalesce(F.col("customer.city"), F.lit("Unknown")).alias("customer_city"),
            F.coalesce(F.col("customer.state_province_name"), F.lit("Unknown")).alias("customer_state_province"),
            F.coalesce(F.col("customer.country_region_name"), F.lit("Unknown")).alias("customer_country_region"),
            F.col("fact.promotion_key"),
            F.col("promotion.special_offer_id"),
            F.coalesce(F.col("promotion.description"), F.lit("Unknown")).alias("promotion_description"),
            F.coalesce(F.col("promotion.promotion_type"), F.lit("Unknown")).alias("promotion_type"),
            F.coalesce(F.col("promotion.promotion_category"), F.lit("Unknown")).alias("promotion_category"),
            F.col("fact.currency_key"),
            F.col("currency.currency_rate_id"),
            F.coalesce(F.col("currency.from_currency_code"), F.lit("Base")).alias("from_currency_code"),
            F.coalesce(F.col("currency.to_currency_code"), F.lit("Base")).alias("to_currency_code"),
            F.col("fact.sales_territory_key"),
            F.col("territory.territory_id"),
            F.coalesce(F.col("territory.territory_name"), F.lit("Unknown")).alias("territory_name"),
            F.coalesce(F.col("territory.territory_group"), F.lit("Unknown")).alias("territory_group"),
            F.coalesce(F.col("territory.country_region_code"), F.lit("Unknown")).alias("territory_country_region_code"),
            F.col("fact.order_date_key"),
            F.col("order_date.date").alias("order_date"),
            F.col("order_date.year").alias("order_year"),
            F.col("order_date.quarter").alias("order_quarter"),
            F.col("order_date.month").alias("order_month"),
            F.col("order_date.month_name").alias("order_month_name"),
            F.col("fact.due_date_key"),
            F.col("due_date.date").alias("due_date"),
            F.col("fact.ship_date_key"),
            F.col("ship_date.date").alias("ship_date"),
            F.col("fact.order_quantity"),
            F.col("fact.unit_price"),
            gross_sales_amount.alias("gross_sales_amount"),
            F.col("fact.sales_amount").alias("net_sales_amount"),
            discount_amount.alias("discount_amount"),
            null_safe_ratio(discount_amount, gross_sales_amount).alias("discount_rate"),
            F.col("fact.modified_at"),
            F.current_timestamp().alias("__processing_time"),
        )
    )


@dp.table(
    name="bi_sales_monthly",
    comment="Monthly Internet Sales metrics for executive BI reporting.",
    table_properties={
        "clusterBy": "auto",
    },
)
def bi_sales_monthly():
    source = spark.read.table("bi_sales_order_line")
    gross_sales_amount = F.sum("gross_sales_amount")
    net_sales_amount = F.sum("net_sales_amount")
    discount_amount = F.sum("discount_amount")
    order_count = F.countDistinct("sales_order_id")

    return source.groupBy("order_year", "order_month", "order_month_name").agg(
        F.count("*").alias("order_line_count"),
        order_count.alias("order_count"),
        F.countDistinct("customer_id").alias("customer_count"),
        F.sum("order_quantity").alias("order_quantity"),
        gross_sales_amount.alias("gross_sales_amount"),
        net_sales_amount.alias("net_sales_amount"),
        discount_amount.alias("discount_amount"),
        null_safe_ratio(net_sales_amount, order_count).alias("average_order_value"),
        null_safe_ratio(discount_amount, gross_sales_amount).alias("discount_rate"),
    )


@dp.table(
    name="bi_product_performance",
    comment="Internet Sales metrics by product hierarchy.",
    table_properties={
        "clusterBy": "auto",
    },
)
def bi_product_performance():
    source = spark.read.table("bi_sales_order_line")
    gross_sales_amount = F.sum("gross_sales_amount")
    net_sales_amount = F.sum("net_sales_amount")
    discount_amount = F.sum("discount_amount")
    order_count = F.countDistinct("sales_order_id")

    return source.groupBy(
        "product_category_name",
        "product_subcategory_name",
        "product_id",
        "product_name",
        "product_line",
        "product_class",
        "product_style",
        "product_color",
    ).agg(
        F.count("*").alias("order_line_count"),
        order_count.alias("order_count"),
        F.countDistinct("customer_id").alias("customer_count"),
        F.sum("order_quantity").alias("order_quantity"),
        gross_sales_amount.alias("gross_sales_amount"),
        net_sales_amount.alias("net_sales_amount"),
        discount_amount.alias("discount_amount"),
        null_safe_ratio(net_sales_amount, order_count).alias("average_order_value"),
        null_safe_ratio(discount_amount, gross_sales_amount).alias("discount_rate"),
    )


@dp.table(
    name="bi_territory_performance",
    comment="Internet Sales metrics by sales territory.",
    table_properties={
        "clusterBy": "auto",
    },
)
def bi_territory_performance():
    source = spark.read.table("bi_sales_order_line")
    gross_sales_amount = F.sum("gross_sales_amount")
    net_sales_amount = F.sum("net_sales_amount")
    discount_amount = F.sum("discount_amount")
    order_count = F.countDistinct("sales_order_id")

    return source.groupBy(
        "territory_country_region_code",
        "territory_group",
        "territory_id",
        "territory_name",
    ).agg(
        F.count("*").alias("order_line_count"),
        order_count.alias("order_count"),
        F.countDistinct("customer_id").alias("customer_count"),
        F.sum("order_quantity").alias("order_quantity"),
        gross_sales_amount.alias("gross_sales_amount"),
        net_sales_amount.alias("net_sales_amount"),
        discount_amount.alias("discount_amount"),
        null_safe_ratio(net_sales_amount, order_count).alias("average_order_value"),
        null_safe_ratio(discount_amount, gross_sales_amount).alias("discount_rate"),
    )


@dp.table(
    name="bi_customer_segments",
    comment="Internet Sales metrics by customer demographic segments.",
    table_properties={
        "clusterBy": "auto",
    },
)
def bi_customer_segments():
    source = spark.read.table("bi_sales_order_line")
    gross_sales_amount = F.sum("gross_sales_amount")
    net_sales_amount = F.sum("net_sales_amount")
    discount_amount = F.sum("discount_amount")
    order_count = F.countDistinct("sales_order_id")

    return source.groupBy(
        "yearly_income",
        "education",
        "occupation",
        "gender",
        "marital_status",
        "home_owner_flag",
        "commute_distance",
    ).agg(
        F.count("*").alias("order_line_count"),
        order_count.alias("order_count"),
        F.countDistinct("customer_id").alias("customer_count"),
        F.sum("order_quantity").alias("order_quantity"),
        gross_sales_amount.alias("gross_sales_amount"),
        net_sales_amount.alias("net_sales_amount"),
        discount_amount.alias("discount_amount"),
        null_safe_ratio(net_sales_amount, order_count).alias("average_order_value"),
        null_safe_ratio(discount_amount, gross_sales_amount).alias("discount_rate"),
    )

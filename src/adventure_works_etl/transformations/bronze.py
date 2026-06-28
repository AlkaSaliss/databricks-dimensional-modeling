from pyspark import pipelines as dp
from pyspark.sql import functions as F


RAW_DATA_PATH = "/Volumes/workspace/default/adventure_works_dump"
list_tables = [
    "sales.salesorderheader",
    "sales.salesorderdetail",
    "sales.specialoffer",
    "sales.specialofferproduct",
    "sales.currencyrate",
    "sales.salesterritory",
    "production.product",
    "production.productsubcategory",
    "production.productcategory",
    "person.person",
    "person.emailaddress",
    "person.address",
    "person.stateprovince",
    "person.countryregion",
    "sales.customer",
]


def bronze_ingestion_factory(src_table):
    src_path = f"{RAW_DATA_PATH}/{src_table}"
    sch, tbl = src_table.split(".")
    tgt_table_name = f"bronze_{sch}_{tbl}"

    dp.create_streaming_table(
        name=tgt_table_name,
        comment=f"Raw data extraction for table: {src_table}",
        table_properties={
            "clusterBy": "auto",
        },
    )

    @dp.append_flow(
        target=tgt_table_name,
        name=f"{tgt_table_name}_flow",
    )
    def bronze_ingestion():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(src_path)
            .withColumn("__source_file_name", F.col("_metadata.file_path"))
            .withColumn("__ingestion_time", F.current_timestamp())
        )

    @dp.view(
        name=f"v_{tgt_table_name}_count_verification",
        comment=f"Count verification for table: {tgt_table_name}",
    )
    @dp.expect_or_fail(
        name=f"{tgt_table_name}_count_verification",
        inv="count_src == count_tgt",
    )
    def count_verification():
        return (
            spark.createDataFrame(
                [[spark.read.format("json").load(src_path).count(), spark.table(tgt_table_name).count()]],
                ["count_src", "count_tgt"]
            )
        )

    return bronze_ingestion


for src_table in list_tables:
    bronze_ingestion_factory(src_table)

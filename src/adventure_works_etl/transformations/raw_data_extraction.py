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


def raw_data_extraction_factory(src_table):
    src_path = f"{RAW_DATA_PATH}/{src_table}"
    src_table_name = src_table.split(".")[-1]
    tgt_table_name = f"{src_table_name}_bronze"

    dp.create_streaming_table(
        name=tgt_table_name,
        comment=f"Raw data extraction for table: {src_table_name}",
        table_properties={
            "clusterBy": "auto",
        },
    )

    @dp.append_flow(
        target=tgt_table_name,
        name=f"{tgt_table_name}_flow",
    )
    def raw_data_extraction():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            # .option("cloudFiles.useManagedFileEvents", True)
            .load(src_path)
            .withColumn("source_file_name", F.col("_metadata.file_path"))
            .withColumn("_ingestion_time", F.current_timestamp())
        )

    @dp.view(
        name=f"v_{tgt_table_name}_count_verification",
        comment=f"Count verification for table: {src_table_name}",
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

    return raw_data_extraction


for src_table in list_tables:
    raw_data_extraction_factory(src_table)

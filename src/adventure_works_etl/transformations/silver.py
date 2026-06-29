from pyspark import pipelines as dp
from pyspark.sql import functions as F

from transformations.helpers import individual_survey_field, parse_spatial_location


@dp.table(
    name="silver_person",
    comment="Cleaned incremental Person.Person records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {"valid_business_entity_id": "business_entity_id IS NOT NULL", "valid_modified_at": "modified_at IS NOT NULL"}
)
def silver_person():
    demographics = F.col("Demographics").cast("string")
    total_purchase_ytd = individual_survey_field(demographics, "TotalPurchaseYTD")
    date_first_purchase = individual_survey_field(demographics, "DateFirstPurchase")
    birth_date = individual_survey_field(demographics, "BirthDate")
    marital_status = individual_survey_field(demographics, "MaritalStatus")
    yearly_income = individual_survey_field(demographics, "YearlyIncome")
    gender = individual_survey_field(demographics, "Gender")
    total_children = individual_survey_field(demographics, "TotalChildren")
    number_children_at_home = individual_survey_field(demographics, "NumberChildrenAtHome")
    education = individual_survey_field(demographics, "Education")
    occupation = individual_survey_field(demographics, "Occupation")
    home_owner_flag = individual_survey_field(demographics, "HomeOwnerFlag")
    number_cars_owned = individual_survey_field(demographics, "NumberCarsOwned")
    commute_distance = individual_survey_field(demographics, "CommuteDistance")

    return spark.readStream.table("bronze_person_person").select(
        F.col("BusinessEntityID").cast("long").alias("business_entity_id"),
        F.col("PersonType").cast("string").alias("person_type"),
        F.col("NameStyle").cast("boolean").alias("name_style"),
        F.col("Title").cast("string").alias("title"),
        F.col("FirstName").cast("string").alias("first_name"),
        F.col("MiddleName").cast("string").alias("middle_name"),
        F.col("LastName").cast("string").alias("last_name"),
        F.col("Suffix").cast("string").alias("suffix"),
        F.col("EmailPromotion").cast("int").alias("email_promotion"),
        demographics.alias("demographics_raw"),
        total_purchase_ytd.cast("decimal(18,4)").alias("total_purchase_ytd"),
        F.to_date(date_first_purchase, "yyyy-MM-dd'Z'").alias("date_first_purchase"),
        F.to_date(birth_date, "yyyy-MM-dd'Z'").alias("birth_date"),
        marital_status.alias("marital_status"),
        yearly_income.alias("yearly_income"),
        gender.alias("gender"),
        total_children.cast("int").alias("total_children"),
        number_children_at_home.cast("int").alias("number_children_at_home"),
        education.alias("education"),
        occupation.alias("occupation"),
        (home_owner_flag == F.lit("1")).alias("home_owner_flag"),
        number_cars_owned.cast("int").alias("number_cars_owned"),
        commute_distance.alias("commute_distance"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_person_email_address",
    comment="Cleaned incremental Person.EmailAddress records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_business_entity_id": "business_entity_id IS NOT NULL",
        "valid_email_address_id": "email_address_id IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
    }
)
def silver_person_email_address():
    return spark.readStream.table("bronze_person_emailaddress").select(
        F.col("BusinessEntityID").cast("long").alias("business_entity_id"),
        F.col("EmailAddressID").cast("long").alias("email_address_id"),
        F.col("EmailAddress").cast("string").alias("email_address"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_person_address",
    comment="Cleaned incremental Person.Address records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_address_id": "address_id IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
        "valid_spatial_parse": "spatial_location_raw IS NULL OR (latitude IS NOT NULL AND longitude IS NOT NULL)",
        "valid_latitude_range": "latitude IS NULL OR (latitude BETWEEN -90 AND 90)",
        "valid_longitude_range": "longitude IS NULL OR (longitude BETWEEN -180 AND 180)",
    }
)
def silver_person_address():
    return (
        spark.readStream.table("bronze_person_address")
        .withColumn("spatial", parse_spatial_location(F.col("SpatialLocation")))
        .select(
            F.col("AddressID").cast("long").alias("address_id"),
            F.col("AddressLine1").cast("string").alias("address_line_1"),
            F.col("AddressLine2").cast("string").alias("address_line_2"),
            F.col("City").cast("string").alias("city"),
            F.col("StateProvinceID").cast("long").alias("state_province_id"),
            F.col("PostalCode").cast("string").alias("postal_code"),
            F.col("SpatialLocation").alias("spatial_location_raw"),
            F.col("spatial.latitude").alias("latitude"),
            F.col("spatial.longitude").alias("longitude"),
            F.to_timestamp("ModifiedDate").alias("modified_at"),
            F.col("rowguid").cast("string").alias("row_guid"),
            F.col("__source_file_name"),
            F.col("__ingestion_time"),
            F.current_timestamp().alias("__processing_time"),
        )
    )


@dp.table(
    name="silver_person_state_province",
    comment="Cleaned incremental Person.StateProvince records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_state_province_id": "state_province_id IS NOT NULL",
        "valid_country_region_code": "country_region_code IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
    }
)
def silver_person_state_province():
    return spark.readStream.table("bronze_person_stateprovince").select(
        F.col("StateProvinceID").cast("long").alias("state_province_id"),
        F.col("StateProvinceCode").cast("string").alias("state_province_code"),
        F.col("CountryRegionCode").cast("string").alias("country_region_code"),
        F.col("IsOnlyStateProvinceFlag").cast("boolean").alias("is_only_state_province_flag"),
        F.col("Name").cast("string").alias("state_province_name"),
        F.col("TerritoryID").cast("long").alias("territory_id"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_person_country_region",
    comment="Cleaned incremental Person.CountryRegion records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {"valid_country_region_code": "country_region_code IS NOT NULL", "valid_modified_at": "modified_at IS NOT NULL"}
)
def silver_person_country_region():
    return spark.readStream.table("bronze_person_countryregion").select(
        F.col("CountryRegionCode").cast("string").alias("country_region_code"),
        F.col("Name").cast("string").alias("country_region_name"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


# ---------------------------------------------------------------------
# Production
# ---------------------------------------------------------------------


@dp.table(
    name="silver_product",
    comment="Cleaned incremental Production.Product records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_product_id": "product_id IS NOT NULL",
        "valid_product_number": "product_number IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
        "valid_safety_stock_level": "safety_stock_level >= 0",
        "valid_reorder_point": "reorder_point >= 0",
        "valid_standard_cost": "standard_cost >= 0",
        "valid_list_price": "list_price >= 0",
        "valid_weight": "weight IS NULL OR weight >= 0",
        "valid_days_to_manufacture": "days_to_manufacture >= 0",
        "valid_sell_date_range": "sell_end_date IS NULL OR sell_start_date IS NULL OR sell_end_date >= sell_start_date",
    }
)
def silver_product():
    return spark.readStream.table("bronze_production_product").select(
        F.col("ProductID").cast("long").alias("product_id"),
        F.col("ProductNumber").cast("string").alias("product_number"),
        F.col("Name").cast("string").alias("product_name"),
        F.col("MakeFlag").cast("boolean").alias("make_flag"),
        F.col("FinishedGoodsFlag").cast("boolean").alias("finished_goods_flag"),
        F.col("Color").cast("string").alias("color"),
        F.col("SafetyStockLevel").cast("int").alias("safety_stock_level"),
        F.col("ReorderPoint").cast("int").alias("reorder_point"),
        F.col("StandardCost").cast("decimal(18,4)").alias("standard_cost"),
        F.col("ListPrice").cast("decimal(18,4)").alias("list_price"),
        F.col("Size").cast("string").alias("size"),
        F.col("SizeUnitMeasureCode").cast("string").alias("size_unit_measure_code"),
        F.col("WeightUnitMeasureCode").cast("string").alias("weight_unit_measure_code"),
        F.col("Weight").cast("decimal(18,4)").alias("weight"),
        F.col("DaysToManufacture").cast("int").alias("days_to_manufacture"),
        F.col("ProductLine").cast("string").alias("product_line"),
        F.col("Class").cast("string").alias("class"),
        F.col("Style").cast("string").alias("style"),
        F.col("ProductSubcategoryID").cast("long").alias("product_subcategory_id"),
        F.col("ProductModelID").cast("long").alias("product_model_id"),
        F.to_timestamp("SellStartDate").alias("sell_start_date"),
        F.to_timestamp("SellEndDate").alias("sell_end_date"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_product_subcategory",
    comment="Cleaned incremental Production.ProductSubcategory records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_product_subcategory_id": "product_subcategory_id IS NOT NULL",
        "valid_product_category_id": "product_category_id IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
    }
)
def silver_product_subcategory():
    return spark.readStream.table("bronze_production_productsubcategory").select(
        F.col("ProductSubcategoryID").cast("long").alias("product_subcategory_id"),
        F.col("ProductCategoryID").cast("long").alias("product_category_id"),
        F.col("Name").cast("string").alias("product_subcategory_name"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_product_category",
    comment="Cleaned incremental Production.ProductCategory records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {"valid_product_category_id": "product_category_id IS NOT NULL", "valid_modified_at": "modified_at IS NOT NULL"}
)
def silver_product_category():
    return spark.readStream.table("bronze_production_productcategory").select(
        F.col("ProductCategoryID").cast("long").alias("product_category_id"),
        F.col("Name").cast("string").alias("product_category_name"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


# ---------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------


@dp.table(
    name="silver_sales_customer",
    comment="Cleaned incremental Sales.Customer records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop({"valid_customer_id": "customer_id IS NOT NULL", "valid_modified_at": "modified_at IS NOT NULL"})
def silver_sales_customer():
    return spark.readStream.table("bronze_sales_customer").select(
        F.col("CustomerID").cast("long").alias("customer_id"),
        F.col("PersonID").cast("long").alias("person_id"),
        F.col("StoreID").cast("long").alias("store_id"),
        F.col("TerritoryID").cast("long").alias("territory_id"),
        F.col("AccountNumber").cast("string").alias("account_number"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_sales_order_header",
    comment="Cleaned incremental Sales.SalesOrderHeader records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_sales_order_id": "sales_order_id IS NOT NULL",
        "valid_sales_order_number": "sales_order_number IS NOT NULL",
        "valid_customer_id": "customer_id IS NOT NULL",
        "valid_order_date": "order_date IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
        "valid_due_date": "due_date IS NULL OR due_date >= order_date",
        "valid_ship_date": "ship_date IS NULL OR ship_date >= order_date",
        "valid_sub_total": "sub_total >= 0",
        "valid_tax_amount": "tax_amount >= 0",
        "valid_freight": "freight >= 0",
        "valid_total_due": "total_due >= 0",
    }
)
def silver_sales_order_header():
    return spark.readStream.table("bronze_sales_salesorderheader").select(
        F.col("SalesOrderID").cast("long").alias("sales_order_id"),
        F.col("RevisionNumber").cast("int").alias("revision_number"),
        F.to_timestamp("OrderDate").alias("order_date"),
        F.to_timestamp("DueDate").alias("due_date"),
        F.to_timestamp("ShipDate").alias("ship_date"),
        F.col("Status").cast("int").alias("status"),
        F.col("OnlineOrderFlag").cast("boolean").alias("online_order_flag"),
        F.col("SalesOrderNumber").cast("string").alias("sales_order_number"),
        F.col("PurchaseOrderNumber").cast("string").alias("purchase_order_number"),
        F.col("AccountNumber").cast("string").alias("account_number"),
        F.col("CustomerID").cast("long").alias("customer_id"),
        F.col("SalesPersonID").cast("long").alias("sales_person_id"),
        F.col("TerritoryID").cast("long").alias("territory_id"),
        F.col("BillToAddressID").cast("long").alias("bill_to_address_id"),
        F.col("ShipToAddressID").cast("long").alias("ship_to_address_id"),
        F.col("ShipMethodID").cast("long").alias("ship_method_id"),
        F.col("CreditCardID").cast("long").alias("credit_card_id"),
        F.col("CreditCardApprovalCode").cast("string").alias("credit_card_approval_code"),
        F.col("CurrencyRateID").cast("long").alias("currency_rate_id"),
        F.col("SubTotal").cast("decimal(18,4)").alias("sub_total"),
        F.col("TaxAmt").cast("decimal(18,4)").alias("tax_amount"),
        F.col("Freight").cast("decimal(18,4)").alias("freight"),
        F.col("TotalDue").cast("decimal(18,4)").alias("total_due"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_sales_order_detail",
    comment="Cleaned incremental Sales.SalesOrderDetail records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_sales_order_id": "sales_order_id IS NOT NULL",
        "valid_sales_order_detail_id": "sales_order_detail_id IS NOT NULL",
        "valid_product_id": "product_id IS NOT NULL",
        "valid_special_offer_id": "special_offer_id IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
        "valid_order_quantity": "order_quantity > 0",
        "valid_unit_price": "unit_price >= 0",
        "valid_unit_price_discount": "unit_price_discount BETWEEN 0 AND 1",
        "valid_line_total": "line_total >= 0",
    }
)
def silver_sales_order_detail():
    return spark.readStream.table("bronze_sales_salesorderdetail").select(
        F.col("SalesOrderID").cast("long").alias("sales_order_id"),
        F.col("SalesOrderDetailID").cast("long").alias("sales_order_detail_id"),
        F.col("CarrierTrackingNumber").cast("string").alias("carrier_tracking_number"),
        F.col("OrderQty").cast("int").alias("order_quantity"),
        F.col("ProductID").cast("long").alias("product_id"),
        F.col("SpecialOfferID").cast("long").alias("special_offer_id"),
        F.col("UnitPrice").cast("decimal(18,4)").alias("unit_price"),
        F.col("UnitPriceDiscount").cast("decimal(18,4)").alias("unit_price_discount"),
        F.col("LineTotal").cast("decimal(18,4)").alias("line_total"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_sales_territory",
    comment="Cleaned incremental Sales.SalesTerritory records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_territory_id": "territory_id IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
        "valid_sales_ytd": "sales_ytd >= 0",
        "valid_sales_last_year": "sales_last_year >= 0",
        "valid_cost_ytd": "cost_ytd >= 0",
        "valid_cost_last_year": "cost_last_year >= 0",
    }
)
def silver_sales_territory():
    return spark.readStream.table("bronze_sales_salesterritory").select(
        F.col("TerritoryID").cast("long").alias("territory_id"),
        F.col("Name").cast("string").alias("territory_name"),
        F.col("CountryRegionCode").cast("string").alias("country_region_code"),
        F.col("Group").cast("string").alias("territory_group"),
        F.col("SalesYTD").cast("decimal(18,4)").alias("sales_ytd"),
        F.col("SalesLastYear").cast("decimal(18,4)").alias("sales_last_year"),
        F.col("CostYTD").cast("decimal(18,4)").alias("cost_ytd"),
        F.col("CostLastYear").cast("decimal(18,4)").alias("cost_last_year"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_sales_special_offer",
    comment="Cleaned incremental Sales.SpecialOffer records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_special_offer_id": "special_offer_id IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
        "valid_discount_pct": "discount_pct BETWEEN 0 AND 1",
        "valid_min_qty": "min_qty >= 0",
        "valid_max_qty": "max_qty IS NULL OR max_qty >= min_qty",
        "valid_offer_date_range": "end_date >= start_date",
    }
)
def silver_sales_special_offer():
    return spark.readStream.table("bronze_sales_specialoffer").select(
        F.col("SpecialOfferID").cast("long").alias("special_offer_id"),
        F.col("Description").cast("string").alias("description"),
        F.col("DiscountPct").cast("decimal(10,4)").alias("discount_pct"),
        F.col("Type").cast("string").alias("promotion_type"),
        F.col("Category").cast("string").alias("promotion_category"),
        F.to_timestamp("StartDate").alias("start_date"),
        F.to_timestamp("EndDate").alias("end_date"),
        F.col("MinQty").cast("int").alias("min_qty"),
        F.col("MaxQty").cast("int").alias("max_qty"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_sales_special_offer_product",
    comment="Cleaned incremental Sales.SpecialOfferProduct bridge records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_special_offer_id": "special_offer_id IS NOT NULL",
        "valid_product_id": "product_id IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
    }
)
def silver_sales_special_offer_product():
    return spark.readStream.table("bronze_sales_specialofferproduct").select(
        F.col("SpecialOfferID").cast("long").alias("special_offer_id"),
        F.col("ProductID").cast("long").alias("product_id"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("rowguid").cast("string").alias("row_guid"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )


@dp.table(
    name="silver_sales_currency_rate",
    comment="Cleaned incremental Sales.CurrencyRate records.",
    table_properties={
        "clusterBy": "auto",
    },
)
@dp.expect_all_or_drop(
    {
        "valid_currency_rate_id": "currency_rate_id IS NOT NULL",
        "valid_modified_at": "modified_at IS NOT NULL",
        "valid_from_currency_code": "LENGTH(from_currency_code) = 3",
        "valid_to_currency_code": "LENGTH(to_currency_code) = 3",
        "valid_average_rate": "average_rate > 0",
        "valid_end_of_day_rate": "end_of_day_rate > 0",
    }
)
def silver_sales_currency_rate():
    return spark.readStream.table("bronze_sales_currencyrate").select(
        F.col("CurrencyRateID").cast("long").alias("currency_rate_id"),
        F.to_timestamp("CurrencyRateDate").alias("currency_rate_date"),
        F.col("FromCurrencyCode").cast("string").alias("from_currency_code"),
        F.col("ToCurrencyCode").cast("string").alias("to_currency_code"),
        F.col("AverageRate").cast("decimal(18,6)").alias("average_rate"),
        F.col("EndOfDayRate").cast("decimal(18,6)").alias("end_of_day_rate"),
        F.to_timestamp("ModifiedDate").alias("modified_at"),
        F.col("__source_file_name"),
        F.col("__ingestion_time"),
        F.current_timestamp().alias("__processing_time"),
    )

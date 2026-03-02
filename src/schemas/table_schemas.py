from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

# City Info Schema (CSV source)
city_schema = StructType([
    StructField("city_id", LongType(), True),
    StructField("city_name", StringType(), True),
    StructField("area_name", StringType(), True),
    # Field to capture malformed rows during CSV parsing
    StructField("_corrupt_record", StringType(), True)
])

# Product Info Schema (CSV source)
produce_schema = StructType([
    StructField("produce_id", LongType(), True),
    StructField("produce_name", StringType(), True),
    StructField("extend_info", StringType(), True),
    # Field to capture malformed rows during CSV parsing
    StructField("_corrupt_record", StringType(), True)
])

# User Visit Action Schema (Reference for Parquet)
USER_VISIT_ACTION_SCHEMA = StructType([
    StructField("date", StringType(), True),
    StructField("user_id", LongType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_id", LongType(), True),
    StructField("action_time", StringType(), True),
    StructField("search_keyword", StringType(), True),
    StructField("click_category_id", LongType(), True),
    StructField("click_product_id", LongType(), True),
    StructField("order_category_ids", StringType(), True),
    StructField("order_product_ids", StringType(), True),
    StructField("pay_category_ids", StringType(), True),
    StructField("pay_product_ids", StringType(), True),
    StructField("city_id", LongType(), True)
])

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.transform import clean_data
from src.schemas.table_schemas import city_schema, produce_schema

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession fixture for all tests."""
    return SparkSession.builder.master("local[*]").appName("pytest-pyspark-local-testing").getOrCreate()

def test_run_basic(monkeypatch, spark):
    # Create city_df with explicit schema
    city_raw = [
        Row(city_id=1, city_name="Beijing", area_name="North China"),
        Row(city_id=2, city_name="Shanghai", area_name="East China"),
        Row(city_id=3, city_name="Tianjin", area_name="North China"),
    ]
    city_df = spark.createDataFrame(city_raw, schema=city_schema)

    # Create produce_df with explicit schema
    produce_raw = [
        Row(produce_id=101, produce_name="Apple", extend_info=None),
        Row(produce_id=102, produce_name="Banana", extend_info=None),
    ]
    produce_df = spark.createDataFrame(produce_raw, schema=produce_schema)

    # Create user_visit_action_df (simulating parquet return)
    uva_raw = [
        # North China: Beijing clicks Apple 3 times, Tianjin clicks Apple 1 time
        Row(city_id=1, click_product_id=101, user_id=1, session_id="s1", page_id=1, action_time_ms=0,
            search_keyword=None, click_category_id=None, order_category_ids=None, order_product_ids=None,
            pay_category_ids=None, pay_product_ids=None),
        Row(city_id=1, click_product_id=101, user_id=2, session_id="s2", page_id=1, action_time_ms=0,
            search_keyword=None, click_category_id=None, order_category_ids=None, order_product_ids=None,
            pay_category_ids=None, pay_product_ids=None),
        Row(city_id=1, click_product_id=101, user_id=3, session_id="s3", page_id=1, action_time_ms=0,
            search_keyword=None, click_category_id=None, order_category_ids=None, order_product_ids=None,
            pay_category_ids=None, pay_product_ids=None),
        Row(city_id=3, click_product_id=101, user_id=4, session_id="s4", page_id=1, action_time_ms=0,
            search_keyword=None, click_category_id=None, order_category_ids=None, order_product_ids=None,
            pay_category_ids=None, pay_product_ids=None),
        # East China: Shanghai clicks Banana 2 times, Apple 1 time
        Row(city_id=2, click_product_id=102, user_id=5, session_id="s5", page_id=1, action_time_ms=0,
            search_keyword=None, click_category_id=None, order_category_ids=None, order_product_ids=None,
            pay_category_ids=None, pay_product_ids=None),
        Row(city_id=2, click_product_id=102, user_id=6, session_id="s6", page_id=1, action_time_ms=0,
            search_keyword=None, click_category_id=None, order_category_ids=None, order_product_ids=None,
            pay_category_ids=None, pay_product_ids=None),
        Row(city_id=2, click_product_id=101, user_id=7, session_id="s7", page_id=1, action_time_ms=0,
            search_keyword=None, click_category_id=None, order_category_ids=None, order_product_ids=None,
            pay_category_ids=None, pay_product_ids=None),
    ]
    uva_schema = StructType([
        StructField("city_id", IntegerType(), True),
        StructField("click_product_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("page_id", IntegerType(), True),
        StructField("action_time_ms", IntegerType(), True),
        StructField("search_keyword", StringType(), True),
        StructField("click_category_id", IntegerType(), True),
        StructField("order_category_ids", StringType(), True),
        StructField("order_product_ids", StringType(), True),
        StructField("pay_category_ids", StringType(), True),
        StructField("pay_product_ids", StringType(), True)
    ])
    user_visit_action_df = spark.createDataFrame(uva_raw, schema=uva_schema)

    # Monkeypatch S3 reader functions to return constructed DataFrames
    import src.readers.read_from_s3 as rfs

    def fake_read_s3_csv(spark_session, path, header=False, inferSchema=True, schema=None):
        # Verify schema is passed
        if schema is None:
             pytest.fail("Expected schema to be passed to read_s3_csv")

        # Return city or produce DF based on path
        if "city" in str(path):
            return city_df
        return produce_df

    def fake_read_s3_parquet(spark_session, path):
        return user_visit_action_df

    monkeypatch.setattr(rfs, "read_s3_csv", fake_read_s3_csv)
    monkeypatch.setattr(rfs, "read_s3_parquet", fake_read_s3_parquet)

    # Simple configs (paths are ignored by monkeypatched functions)
    configs = {
        "input": {
            "city_path": "city_path",
            "produce_path": "produce_path",
            "user_visit_action_path": "user_visit_action_path"
        }
    }

    # Run logic
    result_df = clean_data.run(spark, configs)

    # Collect and sort results for comparison
    actual = sorted([row.asDict() for row in result_df.collect()], key=lambda r: (r['area_name'], r['produce_name']))

    expected = [
        {"area_name": "North China", "produce_name": "Apple", "total_clicks": 4, "city_remark": "Beijing75.0%, Tianjin25.0%"},
        {"area_name": "East China", "produce_name": "Banana", "total_clicks": 2, "city_remark": "Shanghai100.0%"},
        {"area_name": "East China", "produce_name": "Apple", "total_clicks": 1, "city_remark": "Shanghai100.0%"},
    ]
    expected = sorted(expected, key=lambda r: (r['area_name'], r['produce_name']))

    assert actual == expected

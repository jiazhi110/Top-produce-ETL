import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
from src.transform import clean_data
from src.schemas.table_schemas import city_schema, produce_schema

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession fixture for all tests."""
    return SparkSession.builder.master("local[*]").appName("pytest-pyspark-local-testing").getOrCreate()

def test_run_with_dlq(monkeypatch, spark):
    """
    Test the main transformation logic and ensure DLQ captures corrupt records.
    """
    # 1. Prepare Mock Data for Dimension Tables (CSVs)
    city_raw = [
        Row(city_id=1, city_name="Beijing", area_name="North China", _corrupt_record=None),
        Row(city_id=2, city_name="Shanghai", area_name="East China", _corrupt_record=None),
    ]
    city_df = spark.createDataFrame(city_raw, schema=city_schema)

    produce_raw = [
        Row(produce_id=101, produce_name="Apple", extend_info=None, _corrupt_record=None),
        # Simulated parsing error: corrupt record captured by PERMISSIVE mode
        Row(produce_id=None, produce_name=None, extend_info=None, _corrupt_record="BAD_CSV_LINE_123"),
    ]
    produce_df = spark.createDataFrame(produce_raw, schema=produce_schema)

    # 2. Prepare Mock Data for Fact Table (user_visit_action)
    # Define explicit schema to avoid inference errors with Null values
    uva_schema = StructType([
        StructField("city_id", LongType(), True),
        StructField("click_product_id", LongType(), True),
        StructField("user_id", LongType(), True),
        StructField("session_id", StringType(), True),
        StructField("page_id", LongType(), True),
        StructField("action_time_ms", LongType(), True),
        StructField("search_keyword", StringType(), True),
        StructField("click_category_id", LongType(), True),
        StructField("order_category_ids", StringType(), True),
        StructField("order_product_ids", StringType(), True),
        StructField("pay_category_ids", StringType(), True),
        StructField("pay_product_ids", StringType(), True)
    ])

    uva_raw = [
        # Beijing (North China) - 2 clicks for Apple
        (1, 101, 1, "s1", 1, 0, None, None, None, None, None, None),
        (1, 101, 2, "s2", 1, 0, None, None, None, None, None, None),
        # Shanghai (East China) - 1 click for Apple
        (2, 101, 3, "s3", 1, 0, None, None, None, None, None, None),
    ]
    user_visit_action_df = spark.createDataFrame(uva_raw, schema=uva_schema)

    # 3. Mock S3 Readers using monkeypatch
    import src.readers.read_from_s3 as rfs

    def fake_read_s3_csv(spark_session, path, header=False, inferSchema=True, schema=None):
        if "city" in str(path): return city_df
        return produce_df

    def fake_read_s3_parquet(spark_session, path):
        return user_visit_action_df

    monkeypatch.setattr(rfs, "read_s3_csv", fake_read_s3_csv)
    monkeypatch.setattr(rfs, "read_s3_parquet", fake_read_s3_parquet)
    monkeypatch.setattr(rfs, "read_s3_parquet_with_bookmark", 
                        lambda ctx, path, node, partition_filter=None: user_visit_action_df)

    # 4. Execute Business Logic
    configs = {
        "input": {
            "city_path": "city.csv",
            "produce_path": "produce.csv",
            "user_visit_action_path": "action.parquet"
        }
    }
    
    # Run the ETL logic (returns result and bad records)
    result_df, bad_records_df = clean_data.run(spark, configs)

    # 5. Assertions
    # Verify business logic (Top 3)
    results = result_df.collect()
    assert len(results) > 0
    # verify Beijing data
    beijing_res = [r for r in results if r['area_name'] == 'North China'][0]
    assert beijing_res['total_clicks'] == 2
    assert "Beijing" in beijing_res['city_remark']

    # Verify DLQ (One bad record from produce_df)
    assert bad_records_df.count() == 1
    assert bad_records_df.collect()[0]['source'] == "produce_info"

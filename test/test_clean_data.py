import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.transform import clean_data

@pytest.fixture(scope="session")
def spark():
    """为所有测试创建一个可用的SparkSession fixture"""
    return SparkSession.builder.master("local[*]").appName("pytest-pyspark-local-testing").getOrCreate()

def test_run_basic(monkeypatch, spark):
    # 构造 city_df（注意：run 会对返回的 df 调用 toDF(*city_columns)）
    city_raw = [
        Row(_c0=1, _c1="北京", _c2="华北"),
        Row(_c0=2, _c1="上海", _c2="华东"),
        Row(_c0=3, _c1="天津", _c2="华北"),
    ]
    city_schema = StructType([
        StructField("_c0", IntegerType(), True),
        StructField("_c1", StringType(), True),
        StructField("_c2", StringType(), True)
    ])
    city_df = spark.createDataFrame(city_raw, schema=city_schema)

    # 构造 produce_df（run 会对返回的 df 调用 toDF(*produce_columns)）
    produce_raw = [
        Row(_c0=101, _c1="苹果", _c2=None),
        Row(_c0=102, _c1="香蕉", _c2=None),
    ]
    produce_schema = StructType([
        StructField("_c0", IntegerType(), True),
        StructField("_c1", StringType(), True),
        StructField("_c2", StringType(), True)
    ])
    produce_df = spark.createDataFrame(produce_raw, schema=produce_schema)

    # 构造 user_visit_action_df（parquet 格式返回）
    uva_raw = [
        # 华北: 北京点击3次苹果, 天津点击1次苹果
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
        # 华东: 上海点击2次香蕉, 1次苹果
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

    # monkeypatch S3 reader 函数，直接返回上面构造的 DataFrame
    import src.readers.read_from_s3 as rfs

    def fake_read_s3_csv(spark_session, path, header=False, inferSchema=True):
        # 根据 path 决定返回 city 或 produce（测试中我们用 'city_path' 和 'produce_path'）
        if "city" in str(path):
            return city_df
        return produce_df

    def fake_read_s3_parquet(spark_session, path):
        return user_visit_action_df

    monkeypatch.setattr(rfs, "read_s3_csv", fake_read_s3_csv)
    monkeypatch.setattr(rfs, "read_s3_parquet", fake_read_s3_parquet)

    # 简单 configs，路径字符串不会被真实使用（被 monkeypatched 的函数忽略）
    configs = {
        "input": {
            "city_path": "city_path",
            "produce_path": "produce_path",
            "user_visit_action_path": "user_visit_action_path"
        }
    }

    # 调用目标函数
    result_df = clean_data.run(spark, configs)

    # 收集并比较结果（按 area_name, produce_name 排序以避免顺序问题）
    actual = sorted([row.asDict() for row in result_df.collect()], key=lambda r: (r['area_name'], r['produce_name']))

    expected = [
        {"area_name": "华北", "produce_name": "苹果", "total_clicks": 4, "city_remark": "北京75.0%，天津25.0%"},
        {"area_name": "华东", "produce_name": "香蕉", "total_clicks": 2, "city_remark": "上海100.0%"},
        {"area_name": "华东", "produce_name": "苹果", "total_clicks": 1, "city_remark": "上海100.0%"},
    ]
    expected = sorted(expected, key=lambda r: (r['area_name'], r['produce_name']))

    assert actual == expected

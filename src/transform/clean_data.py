import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import yaml
from src.readers import read_from_s3
from src.schemas.table_schemas import city_schema, produce_schema
import logging

# Initialize logger
logger = logging.getLogger(__name__)

def run(spark: SparkSession, configs: yaml):
    
    logger.info(f"clean data's configs: {configs}")

    # Read data from S3 using explicit schemas
    city_df = read_from_s3.read_s3_csv(spark, configs['input']['city_path'], header=False, schema=city_schema)
    produce_df = read_from_s3.read_s3_csv(spark, configs['input']['produce_path'], header=False, schema=produce_schema)

    user_visit_action_df = read_from_s3.read_s3_parquet(spark, configs['input']['user_visit_action_path'])
    
    # Filter logic: Identify 'click' behaviors
    # Filter out invalid product IDs (-1, null, empty strings)
    click_condition = (
        F.col('click_product_id').isNotNull() & 
        (F.col('click_product_id').cast("string") != '') & 
        (F.upper(F.col('click_product_id').cast("string")) != 'NULL') & 
        (F.col('click_product_id').cast("string") != '-1')
    )

    user_visit_action_df = user_visit_action_df.withColumn(
        'behavior',
        F.when(click_condition, 'click').otherwise('other')
    )
    
    # Keep only click data
    user_visit_action_df = user_visit_action_df.filter(F.col("behavior") == 'click')

    # Drop unnecessary columns to optimize memory
    user_visit_action_df = user_visit_action_df.drop(
        "user_id", "session_id", "page_id", "action_time_ms", "search_keyword", 
        "click_category_id", "order_category_ids", "order_product_ids", 
        "pay_category_ids", "pay_product_ids"
    )

    city_df.createOrReplaceTempView("city")
    produce_df.createOrReplaceTempView("produce")
    user_visit_action_df.createOrReplaceTempView("user_activity")

    user_behavior_wide = spark.sql("""
        select user_activity.click_product_id, city.city_name, city.area_name, produce.produce_name
        from user_activity
        left join city on user_activity.city_id = city.city_id
        left join produce on user_activity.click_product_id = produce.produce_id
    """)

    user_behavior_wide.createOrReplaceTempView("user_behavior_wide")

    user_city_product_count = spark.sql("""
        select count(click_product_id) click_nums, area_name, city_name, produce_name
        from user_behavior_wide
        group by area_name, city_name, produce_name
        order by click_nums desc
    """)

    user_city_product_count.createOrReplaceTempView("user_city_product_count")

    product_area_city_ratio_percent = spark.sql("""
        -- Step 1: Calculate city-level click distribution
        with city_ratio as (
            select
                area_name,
                produce_name,
                city_name,
                click_nums,
                round(click_nums * 100.0 / sum(click_nums) over (partition by area_name, produce_name), 1) as ratio_percent,
                row_number() over (partition by area_name, produce_name order by click_nums desc, city_name ASC) as city_rn,
                count(*) over (partition by area_name, produce_name) as city_cnt
            from user_city_product_count
        ),
        -- Step 2: Format city distribution string for each product
        product_city_str as (
            select
                area_name,
                produce_name,
                sum(click_nums) as total_clicks,
                -- Format top 2 cities as string (collect_list -> array, concat_ws -> string)
                CONCAT_WS('，',
                    transform(
                        slice(
                            array_sort(
                                collect_list(
                                    named_struct('city_rn', city_rn, 's', concat(city_name, ratio_percent, '%'))
                                )
                            ),
                            1, 2
                        ),
                        x -> x.s
                    )
                ) AS top2_str,

                -- Aggregate remaining cities as "Other"
                CONCAT('Other', CAST(ROUND(SUM(CASE WHEN city_rn > 2 THEN ratio_percent ELSE 0 END), 1) AS STRING), '%') AS other_str,

                MAX(city_cnt) AS city_cnt
            FROM city_ratio
            group by area_name, produce_name
        ),
        -- Step 3: Rank products per area (Top 3)
        ranked_product as (
            select
                area_name,
                produce_name,
                total_clicks,
                CASE 
                    WHEN city_cnt > 2 THEN CONCAT(top2_str, '，', other_str)
                    ELSE top2_str
                END AS city_remark,
                row_number() over (partition by area_name order by total_clicks desc) as rn
            from product_city_str
        )
        -- Step 4: Final selection
        select
            area_name,
            produce_name,
            total_clicks,
            city_remark
        from ranked_product
        where rn <= 3
        order by area_name, rn
    """)

    # product_area_city_ratio_percent.show(100, truncate=True)

    product_area_city_ratio_percent.createOrReplaceTempView("product_area_city_ratio_percent")

    return product_area_city_ratio_percent


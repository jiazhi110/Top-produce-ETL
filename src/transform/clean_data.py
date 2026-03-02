import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import yaml
from src.readers import read_from_s3
from src.schemas.table_schemas import city_schema, produce_schema
import logging

# Initialize logger
logger = logging.getLogger(__name__)

def run(spark: SparkSession, configs: yaml, glue_context=None, partition_filter=None, bookmark_node=None):
    
    logger.info(f"Executing clean_data logic with config: {configs}")

    # --- 1. Load Data ---
    city_df = read_from_s3.read_s3_csv(spark, configs['input']['city_path'], header=False, schema=city_schema)
    produce_df = read_from_s3.read_s3_csv(spark, configs['input']['produce_path'], header=False, schema=produce_schema)

    # --- 2. Data Quality Check (DLQ Extraction for Parsing Errors) ---
    city_bad = city_df.filter("_corrupt_record IS NOT NULL").select(
        F.lit("city_info").alias("source"),
        F.col("_corrupt_record").alias("raw_content"),
        F.current_timestamp().alias("detected_at")
    )
    produce_bad = produce_df.filter("_corrupt_record IS NOT NULL").select(
        F.lit("produce_info").alias("source"),
        F.col("_corrupt_record").alias("raw_content"),
        F.current_timestamp().alias("detected_at")
    )
    
    bad_records_df = city_bad.union(produce_bad)
    
    bad_count = bad_records_df.count()
    if bad_count > 0: 
        logger.warning(f"Detected {bad_count} corrupt rows in CSV dimension tables!")
    
    # Filter for clean records only
    city_df = city_df.filter("_corrupt_record IS NULL").drop("_corrupt_record")
    produce_df = produce_df.filter("_corrupt_record IS NULL").drop("_corrupt_record")

    # --- 3. Load Fact Table (Incremental vs. Full) ---
    if glue_context:
        # Use Glue Job Bookmark and Partition Pruning
        user_visit_action_df = read_from_s3.read_s3_parquet_with_bookmark(
            glue_context, 
            configs['input']['user_visit_action_path'],
            bookmark_node_name=bookmark_node,
            partition_filter=partition_filter
        )
    else:
        # Local Spark mode
        user_visit_action_df = read_from_s3.read_s3_parquet(spark, configs['input']['user_visit_action_path'])
        if partition_filter:
            logger.info(f"Applying manual partition filter for local run: {partition_filter}")
            user_visit_action_df = user_visit_action_df.filter(partition_filter)
    
    # --- 4. Logical Data Filtering (Fact Table) ---
    # Identify invalid product IDs (-1, null, empty strings)
    # These are LOGICAL bad records, not parsing errors
    valid_click_condition = (
        F.col('click_product_id').isNotNull() & 
        (F.col('click_product_id').cast("string") != '') & 
        (F.upper(F.col('click_product_id').cast("string")) != 'NULL') & 
        (F.col('click_product_id').cast("string") != '-1')
    )

    user_visit_action_df = user_visit_action_df.withColumn(
        'behavior',
        F.when(valid_click_condition, 'click').otherwise('other')
    )
    
    # Filter for clicks only for Top 3 calculation
    user_visit_action_df = user_visit_action_df.filter(F.col("behavior") == 'click')

    # Optimization: Drop unnecessary columns to reduce memory overhead
    user_visit_action_df = user_visit_action_df.drop(
        "user_id", "session_id", "page_id", "action_time_ms", "search_keyword", 
        "click_category_id", "order_category_ids", "order_product_ids", 
        "pay_category_ids", "pay_product_ids"
    )

    # --- 5. Data Processing (SQL Logic) ---
    city_df.createOrReplaceTempView("city")
    produce_df.createOrReplaceTempView("produce")
    user_visit_action_df.createOrReplaceTempView("user_activity")

    # Wide Table Join with Broadcast Optimization
    user_behavior_wide = spark.sql("""
        select /*+ BROADCAST(city, produce) */ 
            user_activity.click_product_id, city.city_name, city.area_name, produce.produce_name
        from user_activity
        left join city on user_activity.city_id = city.city_id
        left join produce on user_activity.click_product_id = produce.produce_id
    """)

    user_behavior_wide.createOrReplaceTempView("user_behavior_wide")

    # City-Product counts per Area
    user_city_product_count = spark.sql("""
        select count(click_product_id) click_nums, area_name, city_name, produce_name
        from user_behavior_wide
        group by area_name, city_name, produce_name
        order by click_nums desc
    """)

    user_city_product_count.createOrReplaceTempView("user_city_product_count")

    # Final Top 3 Products per Area with ratio strings
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
        -- Step 2: Format top city distribution string
        product_city_str as (
            select
                area_name,
                produce_name,
                sum(click_nums) as total_clicks,
                CONCAT_WS(', ',
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
                CONCAT('Other', CAST(ROUND(SUM(CASE WHEN city_rn > 2 THEN ratio_percent ELSE 0 END), 1) AS STRING), '%') AS other_str,
                MAX(city_cnt) AS city_cnt
            FROM city_ratio
            group by area_name, produce_name
        ),
        -- Step 3: Filter for Top 3 products per area
        ranked_product as (
            select
                area_name,
                produce_name,
                total_clicks,
                CASE 
                    WHEN city_cnt > 2 THEN CONCAT(top2_str, ', ', other_str)
                    ELSE top2_str
                END AS city_remark,
                row_number() over (partition by area_name order by total_clicks desc) as rn
            from product_city_str
        )
        select
            area_name,
            produce_name,
            total_clicks,
            city_remark
        from ranked_product
        where rn <= 3
        order by area_name, rn
    """)

    return product_area_city_ratio_percent, bad_records_df

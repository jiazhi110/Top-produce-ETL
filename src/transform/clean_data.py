import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import yaml
from src.readers import read_from_s3
import logging

#initialize logger
logging.getLogger(__name__)

def run(spark: SparkSession, configs: yaml):
    
    logging.info(f"clean data's configs: {configs}")

    #read data from s3
    city_df = read_from_s3.read_s3_csv(spark, configs['input']['city_path'], header=False, inferSchema=True)

    produce_df = read_from_s3.read_s3_csv(spark, configs['input']['produce_path'], header=False, inferSchema=True)

    user_visit_action_df = read_from_s3.read_s3_parquet(spark, configs['input']['user_visit_action_path'])

    city_columns = ["city_id", "city_name", "area_name"]

    produce_columns = ["produce_id", "produce_name", "extend_info"]

    city_df = city_df.toDF(*city_columns)

    produce_df = produce_df.toDF(*produce_columns)

    city_df.show()

    produce_df.show()

    # 在用户行为表中，根据click_product_id、order_product_ids，pay_product_ids找出用户行为信息，并取名为behavior字段。
    user_visit_action_df = user_visit_action_df.withColumn('behavior',
                                    F.when(F.col('click_product_id').isNotNull() & (F.col('click_product_id').cast("string") != '') 
                                    & (F.col('click_product_id').cast("string") != 'null') & (F.col('click_product_id').cast("string") != '-1'), 'click')
                                    .otherwise('other'))
    
    # 过滤掉其他的数据，只保留商品点击数据。
    user_visit_action_df = user_visit_action_df.filter(F.col("behavior").isin('click'))

    user_visit_action_df = user_visit_action_df.drop("user_id", "session_id", "page_id",
                              "action_time_ms", "search_keyword", "click_category_id",
                              "order_category_ids", "order_product_ids", "pay_category_ids",
                              "pay_product_ids")
    
    # user_visit_action_df_filter = user_visit_action_df.sample(fraction=0.001, seed=24)

    user_visit_action_df.show(20)

    user_visit_action_df.printSchema()

    city_df.createOrReplaceTempView("city")
    produce_df.createOrReplaceTempView("produce")
    user_visit_action_df.createOrReplaceTempView("user_activity")

    user_behavior_wide = spark.sql("""
        select user_activity.click_product_id, city.city_name, city.area_name, produce.produce_name
        from user_activity
        left join city on user_activity.city_id = city.city_id
        left join produce on user_activity.click_product_id = produce.produce_id
    """)

    user_behavior_wide.show()

    user_behavior_wide.createOrReplaceTempView("user_behavior_wide")

    user_city_product_count = spark.sql("""
        select count(click_product_id) click_nums, area_name, city_name, produce_name
        from user_behavior_wide
        group by area_name, city_name, produce_name
        order by click_nums desc
    """)

    user_city_product_count.show()

    user_city_product_count.createOrReplaceTempView("user_city_product_count")

    product_area_city_ratio_percent = spark.sql("""
        -- Step 1: 计算城市占比明细
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
        -- Step 2: 每个产品生成城市占比字符串
        product_city_str as (
            select
                area_name,
                produce_name,
                sum(click_nums) as total_clicks,
                -- 把前2名收集为字符串（collect_list -> array，concat_ws 把 array->string）
                CONCAT_WS('，',
                    -- COLLECT_LIST(CASE WHEN city_rn <= 2 THEN CONCAT(city_name, ratio_percent, '%') END)
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

                -- 直接求 第3名及以后 的百分比和，作为"其他"
                CONCAT('其他', CAST(ROUND(SUM(CASE WHEN city_rn > 2 THEN ratio_percent ELSE 0 END), 1) AS STRING), '%') AS other_str,

                MAX(city_cnt) AS city_cnt
            FROM city_ratio
            group by area_name, produce_name
        ),
        -- Step 3: 每个地区给产品排序，取前 3
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
        -- Step 4: 最终只取每个地区 top 3 产品
        select
            area_name,
            produce_name,
            total_clicks,
            city_remark
        from ranked_product
        where rn <= 3
        order by area_name, rn
    """)

    product_area_city_ratio_percent.show(100, truncate=True)

    product_area_city_ratio_percent.createOrReplaceTempView("product_area_city_ratio_percent")

    return product_area_city_ratio_percent


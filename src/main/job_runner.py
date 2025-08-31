import sys
import argparse

# --- 从重构后的 helper 模块导入所有需要的函数 ---
from src.utils.spark_helper import (
    detect_environment,
    load_config_from_s3,
    load_config_from_local_file,
    create_spark_session,
    create_glue_context
)
from src.utils.logger import setup_logging
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# --- 导入你的业务逻辑模块 ---
from src.transform import clean_data
from src.writers import write_to_parquet

def parse_local_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--job', required=True, help="need job name to run")
    parser.add_argument('--ven', default="dev", help="environment : dev or prod")

    return parser.parse_args()

def main():
    #initialize
    logger = setup_logging()
    is_glue_env = detect_environment()

    if is_glue_env:
        # --- GLUE 环境 ---
        logger.info("Running in AWS Glue environment.")
        # 添加错误处理来捕获参数解析问题
        try:
            args = getResolvedOptions(sys.argv, ['config_path', 'job_name_param'])
            job_param = args['job_name_param']
            configs = load_config_from_s3(args['config_path'])
        except Exception as e:
            logger.error(f"Failed to parse Glue arguments: {e}")
            logger.error(f"sys.argv: {sys.argv}")
            sys.exit(1)
            
        glue_context, spark = create_glue_context()
        # 初始化Glue作业
        job = Job(glue_context)
        job.init(args['job_name_param'], args)
    else:
        # --- 本地环境 ---
        logger.info("Running in local environment.")
        local_args = parse_local_args()
        job_param = local_args.job
        configs = load_config_from_local_file(local_args.ven)
        spark = create_spark_session(app_name=f"Local_{job_param}")

    # --- 公共 ETL 逻辑 ---
    try:
        logger.info(f"Executing logic for job: '{job_param}'")
        logger.info(f"Executing logic for configs: '{configs}'")
        if job_param == 'top-produce-etl':
            result_df = clean_data.run(spark, configs)
            write_to_parquet.write_df_to_s3(result_df, configs['output']['path'])
        else:
            raise ValueError(f"Job logic for '{job_param}' not found!")
        
        logger.info(f"Job '{job_param}' completed successfully.")
        
        # 在Glue环境中提交作业
        if is_glue_env:
            job.commit()
    except Exception as e:
        logger.error(f"Job '{job_param}' failed: {e}", exc_info=True)
        if is_glue_env:
            job.commit()  # 即使失败也要提交作业以确保日志被写入
        sys.exit(1)

if __name__ == '__main__':
    main()
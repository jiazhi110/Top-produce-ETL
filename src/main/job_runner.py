import sys
from src.utils.context_manager import JobContextManager
from src.transform import clean_data
from src.writers import write_to_parquet
from src.utils.logger import setup_logging

def main():
    # Initialize logger
    logger = setup_logging()
    
    # Initialize Manager
    manager = JobContextManager()
    spark = None

    # --- Common ETL Logic ---
    try:
        spark, glue_context, configs = manager.init_env()
        job_param = manager.args.get('JOB_NAME')

        logger.info(f"Executing logic for job: '{job_param}'")
        if job_param == 'top-produce-etl':
            result_df = clean_data.run(
                spark, 
                configs, 
                glue_context=glue_context,
                partition_filter=manager.partition_filter,
                bookmark_node=manager.bookmark_node
            )
            write_to_parquet.write_df_to_s3(result_df, configs['output']['path'])
        else:
            raise ValueError(f"Job logic for '{job_param}' not found!")
        
        logger.info(f"Job '{job_param}' completed successfully.")
        
        # Commit Glue job ONLY on success (to update bookmarks)
        manager.commit()
        
        if not manager.is_glue:
            print("\n" + "="*50)
            print("Job finished! Local run complete.")
            print("="*50 + "\n")
            
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Always release resources, regardless of success or failure
        if spark:
            spark.stop()
            logger.info("Spark session stopped gracefully.")

if __name__ == '__main__':
    main()
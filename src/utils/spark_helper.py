import os
import yaml
import boto3
from pyspark.sql import SparkSession

# --- Lazy import for Glue libraries ---
# Allows code to run locally even if awsglue is not installed
try:
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    IS_GLUE_AVAILABLE = True
except ImportError:
    GlueContext = None # Placeholder
    IS_GLUE_AVAILABLE = False


# --- Environment Detection ---
def detect_environment():
    """
    Detect if the script is running in an AWS Glue environment.
    Standard practice for Data Engineering workloads.
    """
    try:
        # Attempt to import a module that exists only in AWS Glue
        from awsglue.utils import getResolvedOptions
        return True
    except ImportError:
        # Import failed, assuming local environment
        return False


# --- Config Loading ---
def load_config_from_s3(s3_path: str) -> dict:
    """Load YAML configuration from an S3 path."""
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    response = s3.get_object(Bucket=bucket, Key=key)
    config_content = response['Body'].read().decode('utf-8')
    return yaml.safe_load(config_content)


def load_config_from_local_file(venv: str) -> dict:
    """Load YAML configuration from local filesystem."""
    # Use absolute path to avoid relative path issues
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, f'../../config/config_{venv}.yaml')
    with open(config_path) as f:
        return yaml.safe_load(f)


# --- Spark and Glue Context Creation ---
def _get_project_root() -> str:
    """Calculate and return the project root directory path."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def create_spark_session(app_name: str = "LocalETLJob") -> SparkSession:
    """
    Create and configure SparkSession for local development, specifically for S3a access.
    """
    project_root = _get_project_root()
    jars_folder = os.path.join(project_root, "jars")
    
    # Define required JARs
    required_jars = [
        os.path.join(jars_folder, "hadoop-aws-3.3.2.jar"),
        os.path.join(jars_folder, "aws-java-sdk-bundle-1.11.1026.jar"),
        # commons-configuration is usually a transitive dependency
    ]

    # Check if JARs exist
    for jar_path in required_jars:
        if not os.path.exists(jar_path):
            # Use print instead of logger as logger might not be initialized
            print(f"Warning: Required JAR not found at {jar_path}. Local S3 access may fail.")

    # Join JAR paths with comma
    jars_string = ",".join(required_jars)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # --- Spark Performance and Capability Configs ---
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # --- S3a Connection Configs ---
        .config("spark.jars", jars_string)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Credentials are usually auto-fetched from env vars; explicit provider chain as backup
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    )
    
    spark = builder.getOrCreate()
    return spark


def create_glue_context():
    """
    Create GlueContext and SparkSession in AWS Glue environment.
    """
    if not IS_GLUE_AVAILABLE:
        raise ImportError("Cannot create GlueContext. The required 'awsglue' library is not available.")
    
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    return glue_context, spark
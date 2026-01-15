import pytest
from unittest.mock import MagicMock
from src.readers import read_from_s3

# Note: We do not need a real SparkSession here (unlike in test_clean_data.py).
# We only want to verify if read_from_s3 correctly calls spark.read.csv/parquet.
# We want to avoid real S3 connections or starting a Spark JVM for speed and isolation.
# Therefore, we use unittest.mock to simulate the SparkSession.

@pytest.fixture
def mock_spark():
    """Create a mock SparkSession object."""
    return MagicMock()

def test_read_s3_csv(mock_spark):
    # Case 1: Default behavior (inferSchema=True)
    s3_path = "s3://test-bucket/data.csv"
    mock_df = MagicMock()
    mock_spark.read.csv.return_value = mock_df

    read_from_s3.read_s3_csv(mock_spark, s3_path)
    mock_spark.read.csv.assert_called_with(s3_path, header=True, inferSchema=True)

def test_read_s3_csv_with_schema(mock_spark):
    # Case 2: Explicit Schema (Best Practice)
    s3_path = "s3://test-bucket/data_schema.csv"
    mock_schema = MagicMock()
    mock_df = MagicMock()
    mock_spark.read.csv.return_value = mock_df

    read_from_s3.read_s3_csv(mock_spark, s3_path, schema=mock_schema)
    # When schema is provided, inferSchema should NOT be called
    mock_spark.read.csv.assert_called_with(s3_path, header=True, schema=mock_schema)

def test_read_s3_parquet(mock_spark):
    # Setup
    s3_path = "s3://test-bucket/data.parquet"
    mock_df = MagicMock()
    
    # Configure the mock chain: spark.read.parquet -> returns mock_df
    mock_spark.read.parquet.return_value = mock_df

    # Execute
    result = read_from_s3.read_s3_parquet(mock_spark, s3_path)

    # Verify
    mock_spark.read.parquet.assert_called_once_with(s3_path)
    assert result == mock_df

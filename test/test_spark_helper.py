import pytest
from unittest.mock import MagicMock, patch
from src.utils.spark_helper import detect_environment

def test_detect_environment_local():
    # Simulate local environment (ImportError for awsglue)
    with patch.dict('sys.modules', {'awsglue.utils': None}):
        assert detect_environment() is False

def test_detect_environment_glue():
    # Simulate Glue environment (awsglue exists)
    # The function checks for 'from awsglue.utils import getResolvedOptions'
    # So we need to ensure 'awsglue.utils' is importable.
    
    mock_awsglue_utils = MagicMock()
    
    with patch.dict('sys.modules', {'awsglue.utils': mock_awsglue_utils}):
         assert detect_environment() is True
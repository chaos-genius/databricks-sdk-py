import pytest
from unittest.mock import MagicMock
import pandas as pd
from pyspark.sql import SparkSession
from logging import Logger
from databricks.sdk.chaosgenius import CGConfig


@pytest.fixture
def mock_spark_session():
    # Mock the SparkSession
    spark = MagicMock(SparkSession)
    # Mock the SQL execution and return a DataFrame
    mock_df = pd.DataFrame([
        {
            "entity_type": "workspace",
            "entity_id": "123",
            "include_entity": "yes",
            "entity_config": '{"key": "value"}',
        },
        {
            "entity_type": "workspace",
            "entity_id": "231",
            "include_entity": "yes",
            "entity_config": '{"key2": "value2"}',
        }
    ])
    spark.sql.return_value.toPandas.return_value = mock_df
    return spark


@pytest.fixture
def mock_logger():
    # Mock the Logger
    return MagicMock(Logger)


@pytest.fixture
def cg_config(mock_spark_session, mock_logger):
    # Initialize the CGConfig with mocked dependencies
    return CGConfig(sparkSession=mock_spark_session, logger=mock_logger)


def test_initialization(cg_config, mock_spark_session, mock_logger):
    # Test if the initialization config parsing works as expected.
    assert cg_config.df["entity_config"].iloc[0] == {"key": "value"}


def test_get_method(cg_config):
    # Test the get method with no filters
    result = cg_config.get()
    assert not result.empty
    assert len(result) == 2
    assert result.iloc[0]["entity_id"] == "123"
    assert result.iloc[1]["entity_id"] == "231"


def test_get_method_with_filters(cg_config):
    # Test the get method with filters
    result = cg_config.get(entity_type="workspace", include_entity="yes")
    assert not result.empty
    assert result.iloc[0]["entity_id"] == "123"

    # Test with a filter that returns no results
    result = cg_config.get(entity_type="nonexistent")
    assert result.empty


def test_get_ids_method(cg_config):
    # Test the get_ids method
    result = cg_config.get_ids(entity_type="workspace")
    assert result == {"123", "231"}

    # Test with a filter that returns no results
    result = cg_config.get_ids(entity_type="nonexistent")
    assert result == set()


def test_get_method_with_entity_config_filter(cg_config):
    # Test the get method with entity_config_filter
    result = cg_config.get(entity_config_filter={"key": "value"})
    assert not result.empty
    assert result.iloc[0]["entity_id"] == "123"

    # Test with a filter that returns no results
    result = cg_config.get(entity_config_filter={"key": "nonexistent"})
    assert result.empty

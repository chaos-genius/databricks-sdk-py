import time
import pytest
from unittest.mock import MagicMock
import logging
from pyspark.sql import SparkSession
from databricks.sdk.chaosgenius import LogSparkDBHandler  # Replace with the actual module name


@pytest.fixture
def mock_spark_session():
    # Mock the SparkSession
    spark = MagicMock(SparkSession)
    return spark


@pytest.fixture
def log_handler(mock_spark_session):
    # Initialize the LogSparkDBHandler with mocked SparkSession
    handler = LogSparkDBHandler(
        sparkSession=mock_spark_session, buffer_size=2, time_threshold=1000
    )
    yield handler
    handler.close()


@pytest.fixture
def log_handler_low_time_threshold(mock_spark_session):
    # Initialize the LogSparkDBHandler with mocked SparkSession with 1s threshold
    handler = LogSparkDBHandler(
        sparkSession=mock_spark_session, buffer_size=2, time_threshold=1
    )
    yield handler
    handler.close()


def test_emit(log_handler):
    # Test the emit method
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    log_handler.emit(record)
    assert not log_handler.log_queue.empty()


def test_close(log_handler):
    # Test the close method
    log_handler.close()
    assert log_handler.thread.is_alive() is False  # Thread should be stopped


def test_flush_buffer(log_handler, mock_spark_session):
    # Test the buffer flushing mechanism
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    log_handler.emit(record)
    log_handler.emit(record)  # Emit twice to trigger buffer flush due to buffer_size=2

    # Check if the buffer was flushed
    assert log_handler.buffer == []


def test_time_flush(log_handler_low_time_threshold, mock_spark_session):
    # Test the time-based buffer flushing mechanism
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    log_handler_low_time_threshold.emit(record)
    time.sleep(2)  # Sleep for 2 seconds to trigger buffer flush due to time_threshold=1

    # Check if the buffer was flushed
    assert log_handler_low_time_threshold.buffer == []

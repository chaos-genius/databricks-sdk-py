import logging
import time
import traceback
import threading
import queue

import pandas as pd
from pyspark.sql.session import SparkSession


class LogSparkDBHandler(logging.Handler):
    """Log handler which pushes logs to spark.

    `buffer_size` is the size of the buffer where messages are stored before being
    written to the DB. Default is 50.

    `time_threshold` is the seconds between log messages which forces a flush of
    the buffer. Default is 30.

    To manually flush the buffer, call `flush()`.

    To clear the buffer before exiting, call `close()`.

    Note: The implementation of the buffer here is thread-safe.
    """

    def __init__(self, sparkSession: SparkSession, buffer_size=50, time_threshold=30):
        """Initialize the handler."""
        super().__init__()
        self.sparkSession = sparkSession
        self.log_queue = queue.Queue()
        self.buffer_size = buffer_size
        self.time_threshold = time_threshold
        self.last_log_time = None
        self.buffer = []
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._process_log_queue)
        self.thread.start()

        # Initialize the table
        self.sparkSession.sql(
            """
            CREATE TABLE IF NOT EXISTS chaosgenius.default.chaosgenius_logs (
                msg string,
                name string,
                levelname string,
                levelno long,
                pathname string,
                filename string,
                module string,
                lineno long,
                funcName string,
                createdAt double,
                exc_info string
            )
        """
        )

    def emit(self, record):
        try:
            exc_info = (
                ""
                if record.exc_info is None
                else "".join(
                    traceback.format_exception(
                        record.exc_info[0], record.exc_info[1], record.exc_info[2]
                    )
                )
            )
            log_entry = {
                "msg": record.getMessage(),
                "name": record.name,
                "levelname": record.levelname,
                "levelno": record.levelno,
                "pathname": record.pathname,
                "filename": record.filename,
                "module": record.module,
                "lineno": record.lineno,
                "funcName": record.funcName,
                "createdAt": time.time(),
                "exc_info": exc_info,
            }
            self.log_queue.put(log_entry)
        except Exception:
            traceback.print_exc()
            print("Failed to enqueue log record.")

    def _process_log_queue(self):
        while True:
            try:
                log_entry = self.log_queue.get(timeout=self.time_threshold)
                if log_entry is None:  # Sentinel value to stop the thread
                    self._flush_buffer()
                    break
                with self.lock:
                    self.buffer.append(log_entry)
                    current_time = time.time()
                    if len(self.buffer) >= self.buffer_size or (
                        self.last_log_time
                        and (current_time - self.last_log_time) >= self.time_threshold
                    ):
                        self._flush_buffer()
                        self.last_log_time = current_time
            except queue.Empty:
                # Timeout occurred, flush buffer if not empty
                with self.lock:
                    if self.buffer:
                        self._flush_buffer()

    def _flush_buffer(self):
        if not self.buffer:
            return
        try:
            df = pd.DataFrame(self.buffer)
            self.sparkSession.createDataFrame(df).write.saveAsTable(
                "chaosgenius.default.chaosgenius_logs", mode="append"
            )
            self.buffer.clear()
            self.last_log_time = None
        except Exception:
            traceback.print_exc()
            print("LOGGING TO SPARK FAILED!!!")

    def flush(self, retry_count: int = 3):
        while retry_count != 0:
            try:
                with self.lock:
                    self._flush_buffer()
                return
            except Exception as e:
                print(f"Failed to flush buffer: {e}")
                traceback.print_exc()
            retry_count -= 1
        print(f"Failed to flush buffer after {retry_count} retries. Giving up.")

    def close(self):
        # Put the sentinel value in the queue to stop the thread
        self.log_queue.put(None)
        # Wait for the thread to finish processing
        self.thread.join()
        super().close()

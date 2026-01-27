# logging_config.py (Updated)
from loguru import logger
import sys

# 1. Remove the default loguru handler
logger.remove()

# 2. Define a base handler for any logs without a specific module binding (optional)
# logger.add(sys.stderr, level="INFO") # Or use sys.stdout if you prefer

# 3. Define a filter function
def module_filter(module_name):
    """Factory function to create a log filter based on the 'module' tag."""
    def filter_func(record):
        # Check if the 'module' key exists in the extra dict and matches the name
        return record["extra"].get("module") == module_name
    return filter_func

# 4. Add sinks for each module using the filter

# Log file for the 'scheduler' module
logger.add(
    "logs/scheduler.log",
    rotation="10 MB",
    retention="30 days",
    level="DEBUG",
    filter=module_filter("scheduler"),
    enqueue=True # Use enqueue=True for multi-process/thread safety
)

# Log file for the 'model' module
logger.add(
    "logs/model.log",
    rotation="10 MB",
    retention="30 days",
    level="DEBUG",
    filter=module_filter("model"),
    enqueue=True
)

# Log file for the 'notification' module
logger.add(
    "logs/notification.log",
    rotation="100 MB",
    retention="30 days",
    level="DEBUG",
    filter=module_filter("notification"),
    enqueue=True
)

scheduler_logger = logger.bind(module="scheduler")
model_logger = logger.bind(module="model")
notification_logger = logger.bind(module="notification")

# 6. Export the bound loggers
__all__ = ["scheduler_logger", "model_logger", "notification_logger"]
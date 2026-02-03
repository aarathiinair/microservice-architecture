# logging_decorator.py (Updated)
from functools import wraps
import asyncio

# The decorator now takes the logger instance to use
def log_function_call(module_logger):
    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                module_logger.debug(f"Calling {func.__name__} with args={args}, kwargs={kwargs}")
                try:
                    module_logger.info(f"{func.__name__} started ")
                    result = await func(*args, **kwargs)
                    module_logger.info(f"{func.__name__} completed with this result {result}")
                    module_logger.debug(f"{func.__name__} returned {result}")
                    return result
                except Exception as e:
                    # loguru's .exception() automatically captures traceback
                    module_logger.exception(f"Exception in {func.__name__}: {e}")
                    raise
            return wrapper
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                module_logger.debug(f"Calling {func.__name__} with args={args}, kwargs={kwargs}")
                try:
                    module_logger.info(f"{func.__name__} started ")
                    result = func(*args, **kwargs)
                    module_logger.info(f"{func.__name__} completed with this result {result}")
                    module_logger.debug(f"{func.__name__} returned {result}")
                    return result
                except Exception as e:
                    module_logger.exception(f"Exception in {func.__name__}: {e}")
                    raise
            return wrapper
    return decorator
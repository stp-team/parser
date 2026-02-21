"""API tracking decorator and utilities."""

import functools
import logging
from collections.abc import Callable
from typing import Any

from src.services.api_tracker import track_api_call

logger = logging.getLogger(__name__)


def track_endpoint(endpoint: str, method: str = "GET"):
    """Decorator to track API endpoint calls.

    Args:
        endpoint: The API endpoint path (e.g., "/api/employees")
        method: HTTP method (default: "GET")

    Usage:
        @track_endpoint("/api/employees", "GET")
        async def get_employees():
            ...

        @track_endpoint("/api/kpi", "POST")
        async def fetch_kpi_data():
            ...
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            track_api_call(endpoint, method)
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error calling {endpoint}: {e}")
                raise

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            track_api_call(endpoint, method)
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error calling {endpoint}: {e}")
                raise

        # Return appropriate wrapper based on whether function is async
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def track_api_class(cls: type) -> type:
    """Class decorator to automatically track all method calls.

    This decorator will track any method that has 'get_', 'fetch_', or 'post_'
    in its name. The endpoint name is derived from the method name.

    Usage:
        @track_api_class
        class MyAPI:
            async def get_employees(self):
                # Will be tracked as "GET /api/employees"
                ...

            async def fetch_kpi(self):
                # Will be tracked as "GET /api/kpi"
                ...
    """
    for attr_name, attr_value in cls.__dict__.items():
        if callable(attr_value) and any(
            prefix in attr_name.lower()
            for prefix in ["get_", "fetch_", "post_", "put_", "delete_"]
        ):
            # Derive endpoint from method name
            # get_employees -> /api/employees
            # fetch_kpi -> /api/kpi
            method = "GET"
            if "post_" in attr_name.lower():
                method = "POST"
            elif "put_" in attr_name.lower():
                method = "PUT"
            elif "delete_" in attr_name.lower():
                method = "DELETE"

            # Remove prefix and convert to endpoint path
            endpoint_name = attr_name
            for prefix in ["get_", "fetch_", "post_", "put_", "delete_"]:
                if endpoint_name.lower().startswith(prefix):
                    endpoint_name = endpoint_name[len(prefix) :]
                    break

            endpoint = f"/api/{endpoint_name.replace('_', '/')}"

            # Create tracked version
            original_method = getattr(cls, attr_name)
            tracked_method = track_endpoint(endpoint, method)(original_method)
            setattr(cls, attr_name, tracked_method)

    return cls


import asyncio  # noqa: E402 - Needed for async checks

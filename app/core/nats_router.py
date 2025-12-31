import logging
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from typing import Any

from okc_py import Client

from app.core.nats_client import nats_client

logger = logging.getLogger(__name__)


def serialize_object(obj: Any) -> Any:
    """Convert any object to JSON-serializable format"""
    if obj is None:
        return None

    # Handle datetime objects
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    # Handle lists and tuples
    if isinstance(obj, (list, tuple)):
        return [serialize_object(item) for item in obj]

    # Handle dictionaries
    if isinstance(obj, dict):
        return {key: serialize_object(value) for key, value in obj.items()}

    # Handle dataclasses
    if is_dataclass(obj):
        return serialize_object(asdict(obj))

    # Handle objects with model_dump (Pydantic v2)
    if hasattr(obj, "model_dump"):
        return serialize_object(obj.model_dump())

    # Handle objects with dict() method (Pydantic v1)
    if hasattr(obj, "dict") and callable(obj.dict):
        return serialize_object(obj.dict())

    # Handle objects with __dict__ (regular classes)
    if hasattr(obj, "__dict__"):
        return serialize_object(obj.__dict__)

    # Handle basic types that are already JSON serializable
    if isinstance(obj, (str, int, float, bool)):
        return obj

    # Fallback: convert to string
    return str(obj)


class NATSRouter:
    """Router for NATS commands to API methods"""

    def __init__(
        self,
        employees_api,
        kpi_api,
        premium_api,
        sl_api,
        tutors_api,
        tests_api,
    ):
        self.employees_api = employees_api
        self.kpi_api = kpi_api
        self.premium_api = premium_api
        self.sl_api = sl_api
        self.tutors_api = tutors_api
        self.tests_api = tests_api

        # Create dynamic API mapping
        self.api_map = {
            "employees": self.employees_api,
            "kpi": self.kpi_api,
            "premium": self.premium_api,
            "sl": self.sl_api,
            "tutors": self.tutors_api,
            "tests": self.tests_api,
        }

    async def setup_handlers(self) -> None:
        """Setup all NATS command handlers"""
        # Register dynamic API handler (handles commands like "employees.get_employee")
        nats_client.register_handler("api", self._handle_dynamic_api_command)

        # Register utility handlers
        nats_client.register_handler("list_apis", self._handle_list_apis_command)

        # Register backward-compatible specific handlers (legacy)
        nats_client.register_handler("employees", self._handle_employees_command)
        nats_client.register_handler("employee", self._handle_employee_command)

        logger.info("NATS command handlers registered (dynamic + legacy)")

    async def list_available_apis(self) -> dict[str, Any]:
        """List all available APIs and their methods"""
        apis = {}
        for api_name, api_instance in self.api_map.items():
            methods = [
                method for method in dir(api_instance)
                if not method.startswith('_') and callable(getattr(api_instance, method))
            ]
            apis[api_name] = methods
        return apis

    async def _handle_list_apis_command(self) -> dict[str, Any]:
        """Handle list_apis command"""
        logger.info("Listing available APIs and methods")
        return await self.list_available_apis()

    async def _handle_dynamic_api_command(self, target: str, **params) -> dict[str, Any]:
        """
        Handle dynamic API commands in format: api_class.method_name

        Usage:
        {
            "command": "api",
            "params": {
                "target": "employees.get_employee",
                "employee_id": 123
            }
        }
        """
        try:
            # Parse target string (e.g., "employees.get_employee")
            if "." not in target:
                return {"error": f"Invalid target format. Use 'api_class.method_name'. Got: {target}"}

            api_name, method_name = target.split(".", 1)

            # Get API instance
            if api_name not in self.api_map:
                available_apis = list(self.api_map.keys())
                return {"error": f"Unknown API: {api_name}. Available APIs: {available_apis}"}

            api_instance = self.api_map[api_name]

            # Get method from API instance
            if not hasattr(api_instance, method_name):
                available_methods = [
                    method for method in dir(api_instance)
                    if not method.startswith('_') and callable(getattr(api_instance, method))
                ]
                return {"error": f"Unknown method: {method_name} on {api_name}. Available methods: {available_methods}"}

            method = getattr(api_instance, method_name)

            # Call the method with provided parameters
            logger.info(f"Calling {api_name}.{method_name} with params: {params}")

            if callable(method):
                result = await method(**params)
                return serialize_object(result)
            else:
                return {"error": f"{method_name} is not callable on {api_name}"}

        except TypeError as e:
            # Handle parameter mismatches
            return {"error": f"Parameter error calling {target}: {str(e)}"}
        except Exception as e:
            logger.error(f"Error calling {target}: {e}")
            return {"error": f"Error calling {target}: {str(e)}"}

    # Legacy/Specific handlers for backward compatibility
    async def _handle_employees_command(
        self, exclude_fired: bool = False
    ) -> dict[str, Any]:
        """Handle employees command"""
        logger.info(f"Processing employees command with exclude_fired={exclude_fired}")
        employees = await self.employees_api.get_employees(exclude_fired=exclude_fired)

        if employees is None:
            return {"error": "Failed to fetch employees"}

        # Convert employees to dict for JSON serialization
        return {
            "count": len(employees),
            "employees": serialize_object(employees),
        }

    async def _handle_employee_command(
        self,
        employee_id: int = None,
        employee_fullname: str = None,
        show_kpi: bool = True,
        show_criticals: bool = True,
    ) -> dict[str, Any]:
        """Handle individual employee command"""
        logger.info(
            f"Processing employee command for ID={employee_id}, name={employee_fullname}"
        )

        employee_data = await self.employees_api.get_employee(
            employee_id=employee_id,
            employee_fullname=employee_fullname,
            show_kpi=show_kpi,
            show_criticals=show_criticals,
        )

        if employee_data is None:
            return {"error": "Employee not found or failed to fetch employee data"}

        # Convert employee data to dict for JSON serialization
        return serialize_object(employee_data)


async def setup_nats_router(
    okc_client: Client,
) -> NATSRouter:
    """Setup and configure NATS router"""
    router = NATSRouter(
        employees_api=okc_client.dossier,
        kpi_api=okc_client.ure,
        premium_api=okc_client.premium,
        sl_api=okc_client.sl,
        tutors_api=okc_client.tutors,
        tests_api=okc_client.tests,
    )

    await router.setup_handlers()
    return router

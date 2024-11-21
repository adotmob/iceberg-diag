from typing import List, Optional, Dict, Any

import requests

from icebergdiag.metrics.table import Table


class IcebergDiagnosticsError(Exception):
    """Base class for all exceptions raised by the Iceberg Diagnostics Manager."""

    def __init__(self, message: str = "An error occurred in Iceberg Diagnostics Manager"):
        super().__init__(message)

class SSOAuthenticationError(IcebergDiagnosticsError):
    """Exception raised when the SSO session is expired or invalid."""

    def __init__(self, original_exception: Exception):
        message = str(original_exception)
        super().__init__(message)


class NoRegionError(IcebergDiagnosticsError):
    """Exception raised when no AWS region is specified."""

    def __init__(self):
        super().__init__("No AWS region specified.")


class EndpointConnectionError(IcebergDiagnosticsError):
    """Exception raised when connection to AWS endpoint fails."""

    def __init__(self, region: Optional[str]):
        region_message = f"region '{region}'" if region is not None else "default region"
        super().__init__(f"Could not connect to AWS in the {region_message}.")


class SessionInitializationError(IcebergDiagnosticsError):
    """Exception raised when an AWS session fails to initialize."""

    def __init__(self, original_error: Exception):
        message = f"Failed to initialize AWS session: {original_error}"
        super().__init__(message)


class UnexpectedError(IcebergDiagnosticsError):
    """Exception raised for any unexpected errors."""

    def __init__(self, message: str):
        super().__init__(f"An unexpected error occurred: {message}")


class DatabaseNotFound(IcebergDiagnosticsError):
    """Exception raised for querying non-existent Database."""

    def __init__(self, database: str):
        super().__init__(f"Database does not exist: : {database}")


class TableMetricsCalculationError(IcebergDiagnosticsError):
    """Exception raised when calculating metrics failed"""

    def __init__(self, table: Table, original_exception: Exception):
        self.table = table
        self.original_exception = original_exception
        super().__init__(f"Failed to calculate metrics for table '{table}': {original_exception}")


class ParsingResponseError(IcebergDiagnosticsError):
    def __init__(self, data: Dict[str, Any], table_names: List[str], error: Exception):
        super().__init__(f"Failed to parse diagnostics response {data} for tables: {table_names}: {error}")

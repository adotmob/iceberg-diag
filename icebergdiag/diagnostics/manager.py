import fnmatch
import logging
import os
import traceback
from itertools import chain
from typing import List, Iterable, Dict, Any, Optional, Tuple

import boto3
import botocore.exceptions as boto3_exceptions
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.manifest import DataFile
from pyiceberg.table import Table as IcebergTable, _open_manifest
from pyiceberg.utils.concurrent import ExecutorFactory

from icebergdiag.exceptions import EndpointConnectionError, \
    IcebergDiagnosticsError, TableMetricsCalculationError, SSOAuthenticationError, \
    SessionInitializationError
from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metrics import TableMetrics, MetricsCalculator

logger = logging.getLogger(__name__)


class IcebergDiagnosticsManager:
    def __init__(self, catalog_uri: str, region: Optional[str] = None):
        logger.debug(f"Initializing with region={region if region else 'default'}")
        self.catalog_uri = catalog_uri
        self.catalog_name = 'Iceberg 🧊'
        self.region = region
        self._initialize_catalog()

    def _initialize_catalog(self):
        logger.debug("Starting catalog initialization")
        try:
            self.session = boto3.Session(region_name=self.region)
            credentials = self.session.get_credentials().get_frozen_credentials()
            self.catalog = load_catalog(self.catalog_name, **{
                'uri': self.catalog_uri,
                's3.access-key-id': credentials.access_key,
                's3.secret-access-key': credentials.secret_key,
                's3.region': self.region,
            })
            logger.debug(f"{self.catalog_name} Catalog initialized successfully")
        except boto3_exceptions.EndpointConnectionError:
            raise EndpointConnectionError(self.region)
        except boto3_exceptions.SSOError as e:
            raise SSOAuthenticationError(e) from e
        except boto3_exceptions.BotoCoreError as e:
            raise SessionInitializationError(e)
        except Exception as e:
            raise IcebergDiagnosticsError(f"An unexpected error occurred: {e}")

    def list_databases(self) -> List[str]:
        databases = self.catalog.list_namespaces()
        return sorted([db[0] for db in databases])

    def list_tables(self, database: str) -> List[str]:
        return self._fetch_and_filter_tables(database)

    def get_matching_tables(self, database: str, search_pattern: str) -> List[str]:
        logger.debug(f"Searching for tables in database '{database}' with pattern '{search_pattern}'")
        all_tables = self.list_tables(database)
        return fnmatch.filter(all_tables, search_pattern)

    def calculate_metrics(self, table: Table, file_target_size_mb: int) -> TableMetrics:
        logger.debug(f"Calculating metrics for table: '{table}'", )
        try:
            return TableDiagnostics(self.catalog, table).get_metrics(file_target_size_mb)
        except Exception as e:
            logger.debug(f"Failed to Calculate metrics: {''.join(traceback.format_exception(e))}")
            raise TableMetricsCalculationError(table, e)

    def _fetch_and_filter_tables(self, database: str) -> List[str]:
        iceberg_tables = []
        tables = self.catalog.list_tables(database)
        iceberg_tables.extend([t[1] for t in tables])

        return sorted(iceberg_tables)

    def get_session_info(self) -> Dict[str, Any]:

        credentials = self.session.get_credentials()
        session_info = {
            "accessKey": credentials.access_key,
            "secretKey": credentials.secret_key,
            "region": self.session.region_name,
        }
        if credentials.token:
            session_info["tokenSession"] = credentials.token

        return session_info


class TableDiagnostics:
    def __init__(self, catalog: Catalog, table: Table):
        self.table = table
        self.catalog = catalog

    def get_metrics(self, file_target_size_mb: int) -> TableMetrics:
        files, manifest_files_count = self._get_manifest_files()
        metrics, df = MetricsCalculator.compute_metrics(
            files=files,
            manifest_files_count=manifest_files_count,
            file_target_size_mb=file_target_size_mb,
        )
        return TableMetrics(table=self.table, metrics=metrics, before_and_after_per_partitions=df)

    def _load_table(self) -> IcebergTable:
        logger.debug(f"Loading table {self.table.full_table_name()}")
        return self.catalog.load_table(self.table.full_table_name())

    def _get_manifest_files(self) -> Tuple[Iterable[DataFile], int]:
        logger.debug(f"Getting manifest files for table '{self.table}'")
        """Returns a list of all data files in manifest entries.

        Returns:
            Iterable of DataFile objects.
        """

        def no_filter(_):
            return True

        table = self._load_table()
        logger.debug(f"Scanning table: {self.table.full_table_name()}")
        scan = table.scan()
        logger.debug(f"Loading snapshot for table {self.table.full_table_name()}")
        snapshot = scan.snapshot()
        if not snapshot:
            return iter([]), 0

        io = table.io
        logger.debug(f"Opening manifests files for table {self.table.full_table_name()}")
        manifests = snapshot.manifests(io)
        executor = ExecutorFactory.get_or_create()
        all_data_files = []
        for manifest_entry in chain(
                *executor.map(
                    lambda manifest: _open_manifest(io, manifest, no_filter, no_filter),
                    manifests
                )):
            all_data_files.append(manifest_entry.data_file)

        logger.debug(f"All data loaded successfully for table {self.table.full_table_name()}")
        return all_data_files, len(manifests)

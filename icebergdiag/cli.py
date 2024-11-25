import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Callable, Any, Tuple

from rich import box
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, MofNCompleteColumn
from rich.table import Table as RichTable

from icebergdiag.diagnostics.manager import IcebergDiagnosticsManager
from icebergdiag.exceptions import TableMetricsCalculationError, IcebergDiagnosticsError
from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metric import PartitionsMetricName
from icebergdiag.metrics.table_metrics import TableMetrics
from icebergdiag.metrics.table_metrics_displayer import TableMetricsDisplayer


def configure_logging(verbose=False):
    logging.basicConfig(
        level="ERROR",
        format="%(message)s",
        handlers=[RichHandler(rich_tracebacks=True, show_path=False)]
    )
    if verbose:
        logging.getLogger().setLevel(logging.INFO)
        os.environ['S3FS_LOGGING_LEVEL'] = 'DEBUG'
        debug_loggers = ['icebergdiag', 'pyiceberg']
        for logger_name in debug_loggers:
            logging.getLogger(logger_name).setLevel(logging.DEBUG)


logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='iceberg-diag',
        description='Iceberg Diagnostics Tool')
    parser.add_argument('--region', type=str, help='AWS region')
    parser.add_argument('--catalog-uri', type=str, help='Iceberg Catalog URI')
    parser.add_argument('--database', type=str, help='Database name')
    parser.add_argument('--table-name', type=str, help="Table name or glob pattern (e.g., '*', 'tbl_*')")
    parser.add_argument('--file-target-size-mb', type=int, help='Target size for files, in mb', default=128)
    parser.add_argument('--display-partitions', type=bool, help='Wherever to display additional stats on partitions', default=False)
    parser.add_argument('--display-partitions-limit', type=int, help='Number of lines to display when using additional stats on partitions', default=15)
    parser.add_argument('--display-partitions-sort', type=PartitionsMetricName, help='Column to sort (desc) when using additional stats on partitions', choices=list(PartitionsMetricName), default=PartitionsMetricName.DIFF_OVERHEAD)
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable debug logs')
    return parser.parse_args()


def stderr_print(stderr_message: str) -> None:
    Console(stderr=True).print(stderr_message, style="yellow")


def display_list(lst: List[str], heading: str) -> None:
    console = Console()

    table = RichTable(show_header=True, box=box.SIMPLE_HEAD)
    table.add_column(heading)
    for item in lst:
        table.add_row(item)

    console.print(table)


def list_tables(diagnostics_manager: IcebergDiagnosticsManager, database: str) -> None:
    tables = run_with_progress(diagnostics_manager.list_tables, "Fetching Iceberg tables...", database)
    display_list(tables, "Tables")
    error_message = (
        "Use --table-name to get diagnostics on the Iceberg table, "
        "you can use a glob pattern to receive diagnostics on multiple tables in one command"
    )
    stderr_print(error_message)


def list_databases(diagnostics_manager: IcebergDiagnosticsManager) -> None:
    databases = run_with_progress(diagnostics_manager.list_databases, "Fetching databases...")
    display_list(databases, "Databases")
    stderr_print("Use --database to get the list of tables")


def run_with_progress(task_function, message, *args, **kwargs):
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as progress:
        task_id = progress.add_task(f"[cyan]{message}", total=None)
        result = task_function(*args, **kwargs)
        progress.update(task_id, completed=100)
        return result


def print_table_count_message(table_count, table_pattern):
    console = Console(highlight=False)
    if table_count == 0:
        console.print(f"[bold yellow]No tables matching the pattern '{table_pattern}' were found.[/bold yellow]\n")
    else:
        table_word = "table" if table_count == 1 else "tables"
        wait_message = "please allow a few minutes for the process to complete."
        console.print(f"[green]Analyzing {table_count} {table_word}, {wait_message}[/green]\n")


def fetch_tables(diagnostics_manager: IcebergDiagnosticsManager, database: str, table_pattern: str) -> list[str]:
    tables = run_with_progress(diagnostics_manager.get_matching_tables, "Fetching tables...",
                               database, table_pattern)
    print_table_count_message(len(tables), table_pattern)
    if not tables:
        exit(0)
    return tables


def process_tables(
        diagnostics_manager: IcebergDiagnosticsManager,
        database: str,
        table_pattern: str,
        metric_function: Callable[[Table], Any],
        result_handler: Callable[[TableMetricsDisplayer, Any, List[Tuple[Table, str]]], None]
) -> None:
    table_names = fetch_tables(diagnostics_manager, database, table_pattern)
    tables = [Table(database, name) for name in table_names]
    failed_tables: List[Tuple[Table, str]] = []

    with Progress(SpinnerColumn(spinner_name="line"),
                  TextColumn("{task.description}"),
                  BarColumn(complete_style="progress.percentage"),
                  MofNCompleteColumn(),
                  transient=True) as progress:
        displayer = TableMetricsDisplayer(progress.console)
        with ThreadPoolExecutor(max_workers=1) as executor:  # For some reason, concurrent workers are dumb as hell
            task = progress.add_task("[cyan]Processing...", total=len(tables))
            futures = {executor.submit(metric_function, table): table for table in tables}
            for future in as_completed(futures):
                try:
                    table_result = future.result()
                    result_handler(displayer, table_result, failed_tables)
                except TableMetricsCalculationError as e:
                    failed_tables.append((futures[future], str(e)))

                progress.update(task, advance=1)

    if failed_tables:
        logging.error("Failed to process the following tables:")
        for table, error in failed_tables:
            logging.error(f"Table: {table}, Error: {error}")


def generate_table_metrics(
        diagnostics_manager: IcebergDiagnosticsManager,
        database: str,
        table_pattern: str,
        file_target_size_mb: int,
        display_partitions: bool=True,
        limit_partitions: int=15,
        sort_by_partitions=PartitionsMetricName.DIFF_OVERHEAD,
) -> None:
    def metric_function(table: Table) -> TableMetrics:
        return diagnostics_manager.calculate_metrics(table, file_target_size_mb)

    def result_handler(displayer: TableMetricsDisplayer, table_result: TableMetrics, _) -> None:
        displayer.display_table_metrics(table_result)
        if display_partitions:
            displayer.display_best_partitions_improvements(
                table_metrics=table_result,
                sort_by=sort_by_partitions,
                limit=limit_partitions,
            )
    process_tables(diagnostics_manager, database, table_pattern, metric_function, result_handler)


def cli_runner() -> None:
    args = parse_arguments()
    configure_logging(args.verbose)
    try:
        diagnostics_manager = run_with_progress(
            IcebergDiagnosticsManager,
            "Initializing...",
            catalog_uri=args.catalog_uri,
            region=args.region,
        )

        if args.database is None:
            list_databases(diagnostics_manager)
        elif args.table_name is None:
            list_tables(diagnostics_manager, args.database)
        else:
            generate_table_metrics(
                diagnostics_manager=diagnostics_manager,
                database=args.database,
                table_pattern=args.table_name,
                file_target_size_mb=args.file_target_size_mb,
                display_partitions=args.display_partitions,
                limit_partitions=args.display_partitions_limit,
                sort_by_partitions=args.display_partitions_sort,
            )

    except IcebergDiagnosticsError as e:
        logger.error(e)
        exit(1)
    except KeyboardInterrupt:
        stderr_print("Analysis aborted. Exiting...")
        exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        exit(1)


if __name__ == "__main__":
    cli_runner()

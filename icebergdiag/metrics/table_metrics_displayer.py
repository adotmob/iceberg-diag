from typing import Literal

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box, markup

from icebergdiag.metrics.table_metric import PartitionsMetricName
from icebergdiag.metrics.table_metrics import TableMetrics


class TableMetricsDisplayer:
    """
    Class to display results
    """

    def __init__(self, console: Console):
        self.console = console

    def display_table_metrics(self, table_metrics: TableMetrics) -> None:
        """
        Display table metrics
        :param table_metrics: TableMetrics object
        """
        output_table = TableMetricsDisplayer._create_output_table_metrics(table_metrics.table.full_table_name())
        for metric in table_metrics.metrics:
            output_table.add_row(metric.name.value,
                                 metric.get_before_value(),
                                 metric.get_after_value(),
                                 metric.get_improvement_value())

        panel = Panel(output_table, box=box.MINIMAL)
        self.console.print(panel)

    @staticmethod
    def _create_output_table_metrics(title: str) -> Table:
        """
        Create table metrics with rich
        :param title: Name of table
        :return: Table
        """
        output_table = Table(show_header=True,
                             title=f"Global stats for [bold]{title}[/]")
        output_table.add_column("Metric", justify="left")
        output_table.add_column("Before", justify="right")
        output_table.add_column("After", justify="right")
        output_table.add_column("Improvement", justify="right")
        return output_table

    def display_best_partitions_improvements(self, table_metrics: TableMetrics, sort_by: PartitionsMetricName, limit: int) -> None:
        """
        Display best partitions improvements
        :param table_metrics: Table metrics object
        :param sort_by: Column to sort by, desc
        :param limit: Number of partitions to display
        """
        output_table = TableMetricsDisplayer._create_output_table_partitions(table_metrics.table.full_table_name(), sort_by)
        for d in table_metrics.before_and_after_per_partitions.sort(sort_by, descending=True).limit(limit).to_dicts():
            output_table.add_row(
                markup.escape(d[PartitionsMetricName.PARTITION_NAME]),
                str(d[PartitionsMetricName.OLD_FILE_COUNT]),
                str(d[PartitionsMetricName.NEW_FILE_COUNT]),
                str(d[PartitionsMetricName.DIFF_FILE_COUNT]),
                "{:.2f}%".format(d[PartitionsMetricName.DIFF_FILE_COUNT_PERC]),
                str(d[PartitionsMetricName.OLD_OVERHEAD]),
                str(d[PartitionsMetricName.NEW_OVERHEAD]),
                str(d[PartitionsMetricName.DIFF_OVERHEAD]),
                "{:.2f}%".format(d[PartitionsMetricName.DIFF_OVERHEAD_PERC]),
            )
        panel = Panel(output_table, box=box.MINIMAL)
        self.console.print(panel)

    @staticmethod
    def _create_output_table_partitions(title: str, sort_by: PartitionsMetricName) -> Table:
        """
        Create table metrics with `rich`
        :param title: Name of table
        :param sort_by: Column used for sorting
        :return: Table
        """
        output_table = Table(show_header=True, title=f"Stats per partitions for [bold]{title}[/]")

        col_metrics = [
            PartitionsMetricName.PARTITION_NAME,
            PartitionsMetricName.OLD_FILE_COUNT,
            PartitionsMetricName.NEW_FILE_COUNT,
            PartitionsMetricName.DIFF_FILE_COUNT,
            PartitionsMetricName.DIFF_FILE_COUNT_PERC,
            PartitionsMetricName.OLD_OVERHEAD,
            PartitionsMetricName.NEW_OVERHEAD,
            PartitionsMetricName.DIFF_OVERHEAD,
            PartitionsMetricName.DIFF_OVERHEAD_PERC,
        ]
        for col in col_metrics:
            displayed_value = col.value
            justify: Literal['right'] = 'right'
            if col == sort_by:
                displayed_value += " :downwards_button:"
            if col == PartitionsMetricName.PARTITION_NAME:
                justify: Literal['left'] = 'left'
            output_table.add_column(displayed_value, justify=justify)
        return output_table

from collections import defaultdict
import polars as pl
from typing import Iterable, List, Dict, Tuple

from polars import DataFrame
from pyiceberg.manifest import DataFile, DataFileContent
from pyiceberg.typedef import Record

from icebergdiag.metrics.table import Table
from icebergdiag.metrics.table_metric import TableMetric, MetricName, PartitionsMetricName


class TableMetrics:
    def __init__(self, table: Table, metrics: List[TableMetric], before_and_after_per_partitions: DataFrame):
        self.table = table
        self.metrics = metrics
        self.before_and_after_per_partitions = before_and_after_per_partitions

MEGA_BYTES_TO_BYTES_RATIO = 1024 * 1024
FETCH_SIZE = 32 * MEGA_BYTES_TO_BYTES_RATIO
MILLISECONDS_PER_SCAN = 1


class PartitionMetrics:
    """
    Class to hold metrics for each partition.
    """

    def __init__(self):
        self.file_count = 0
        self.total_size = 0
        self.scan_overhead = 0

    def average_file_size(self):
        """Returns the average file size for the partition."""
        return self.total_size / self.file_count if self.file_count > 0 else 0

    def is_empty(self):
        """Checks if the partition is empty (no files)."""
        return self.file_count == 0


class MetricsCalculator:
    """
    Helper class to calculate table metrics.
    """

    @staticmethod
    def compute_metrics(files: Iterable[Tuple[DataFile, dict]], manifest_files_count: int, file_target_size_mb: int) -> Tuple[List[TableMetric], DataFrame]:
        """Computes various metrics for the table."""
        metrics = {name: 0 for name in MetricName}
        metrics[MetricName.FULL_SCAN_OVERHEAD] = manifest_files_count * MILLISECONDS_PER_SCAN
        partition_files = defaultdict(list)
        partition_metrics = defaultdict(PartitionMetrics)
        total_data_files_count = 0
        total_data_files_size = 0

        for file, partitions in files:
            # partition = MetricsCalculator.deterministic_repr(file.partition)
            partitions_as_str = str(partitions)
            file_size = file.file_size_in_bytes
            read_cost = MetricsCalculator.calc_read_file_cost(file_size)
            overhead = read_cost * MILLISECONDS_PER_SCAN

            metrics[MetricName.FILE_COUNT] += 1
            metrics[MetricName.TOTAL_TABLE_SIZE] += file_size
            metrics[MetricName.FULL_SCAN_OVERHEAD] += overhead

            if file.content == DataFileContent.DATA:
                total_data_files_count += 1
                total_data_files_size += file_size

            partition_metric = partition_metrics[partitions_as_str]
            partition_metric.file_count += 1
            partition_metric.total_size += file_size
            partition_metric.scan_overhead += overhead
            partition_files[partitions_as_str].append(file)

        metrics[MetricName.TOTAL_PARTITIONS] = len(partition_metrics.keys())
        metrics = MetricsCalculator._update_avg_and_worst_metrics(total_data_files_count,
                                                                  total_data_files_size,
                                                                  metrics, partition_metrics)

        after_metrics, worst, before_and_after_per_partitions = MetricsCalculator._compute_after_and_worst_metrics(
            partition_files,
            partition_metrics,
            file_target_size_mb,
        )
        all_metrics = {**metrics, **worst}


        return (
            [TableMetric.create_metric(name, value, after_metrics.get(name)) for name, value in all_metrics.items()],
            TableMetric.compute_stats_per_partitions(before_and_after_per_partitions)
        )

    @staticmethod
    def deterministic_repr(record: Record) -> str:
        """Generate a deterministic string representation of a Record."""
        items = sorted(record.__dict__.items())
        item_strs = [f"{key}={repr(value)}" for key, value in items if not key.startswith('_')]
        return f"{record.__class__.__name__}[{', '.join(item_strs)}]"

    @staticmethod
    def _update_avg_and_worst_metrics(
            data_files_count: int,
            data_files_size: int,
            metrics: Dict[MetricName, int],
            partition_metrics: Dict[str, PartitionMetrics]
    ) -> Dict[MetricName, int]:
        """
        Updates metrics with average and worst-case values based on the consolidated partition data.
        """
        metrics[MetricName.AVG_FILE_SIZE] = (data_files_size / data_files_count) if data_files_count else 0
        metrics[MetricName.WORST_AVG_FILE_SIZE] = min(
            (p.average_file_size() for p in partition_metrics.values() if not p.is_empty()), default=0)
        metrics[MetricName.LARGEST_PARTITION_SIZE] = max((p.total_size for p in partition_metrics.values()), default=0)

        return metrics

    @staticmethod
    def _compute_after_and_worst_metrics(
            partition_file_sizes: Dict[str, List[DataFile]],
            partition_metrics: Dict[str, PartitionMetrics],
            file_target_size_mb: int,
    ) -> Tuple[Dict[MetricName, int], Dict[MetricName, int], pl.DataFrame]:
        """Computes metrics after partitioning."""
        after = {
            MetricName.FILE_COUNT: 0,
            MetricName.WORST_FILE_COUNT: 0,
            MetricName.FULL_SCAN_OVERHEAD: 0,
            MetricName.WORST_SCAN_OVERHEAD: 0,
        }

        worst_partitions = {
            MetricName.WORST_FILE_COUNT: 0,
            MetricName.WORST_SCAN_OVERHEAD: 0
        }


        partition_new_sizes = {}
        for partition, files in partition_file_sizes.items():
            data_files_sizes = [file.file_size_in_bytes for file in files if file.content == DataFileContent.DATA]
            new_partition = MetricsCalculator.build_partition_groups(
                data_files_sizes,
                file_target_size_mb * MEGA_BYTES_TO_BYTES_RATIO,
            )
            partition_cost = sum(MetricsCalculator.calc_read_file_cost(sum(group)) for group in new_partition)
            partition_overhead = partition_cost * MILLISECONDS_PER_SCAN

            partition_new_sizes[partition] = {
                'overhead': partition_overhead,
                'list_files': new_partition,
            }


        max_file_count_reduction = 0
        max_file_scan_reduction = 0
        partition_resume = []
        for partition_name, new_partition in partition_new_sizes.items():
            files_count = len(new_partition['list_files'])
            after[MetricName.FILE_COUNT] += files_count
            partition_overhead = new_partition['overhead']
            after[MetricName.FULL_SCAN_OVERHEAD] += partition_overhead

            file_count_reduction = partition_metrics[partition_name].file_count - files_count
            if file_count_reduction > max_file_count_reduction:
                max_file_count_reduction = file_count_reduction
                worst_partitions[MetricName.WORST_FILE_COUNT] = partition_metrics[partition_name].file_count
                after[MetricName.WORST_FILE_COUNT] = files_count

            file_scan_reduction = partition_metrics[partition_name].scan_overhead - partition_overhead
            if file_scan_reduction > max_file_scan_reduction:
                max_file_scan_reduction = file_scan_reduction
                worst_partitions[MetricName.WORST_SCAN_OVERHEAD] = partition_metrics[partition_name].scan_overhead
                after[MetricName.WORST_SCAN_OVERHEAD] = partition_overhead


            partition_resume.append({
                PartitionsMetricName.PARTITION_NAME: partition_name,
                PartitionsMetricName.OLD_FILE_COUNT: partition_metrics[partition_name].file_count,
                PartitionsMetricName.OLD_OVERHEAD: partition_metrics[partition_name].scan_overhead,
                PartitionsMetricName.NEW_FILE_COUNT: files_count,
                PartitionsMetricName.NEW_OVERHEAD: partition_overhead,
            })

        return after, worst_partitions, pl.DataFrame(partition_resume)

    @staticmethod
    def _create_metric_instances(before_metrics: Dict[MetricName, int],
                                 after_metrics: Dict[MetricName, int]) -> List[TableMetric]:
        """
        Create instances of TableMetric from the metrics dictionary.

        Args:
            before_metrics (Dict[MetricName, int]): Dictionary of 'before' metrics.
            after_metrics (Dict[MetricName, int]): Dictionary of 'after' metrics.

        Returns:
            List[TableMetric]: A list of TableMetric instances.
        """
        metric_instances = []
        for metric_name, before_value in before_metrics.items():
            after_value = after_metrics.get(metric_name)
            metric_instance = TableMetric.create_metric(metric_name, before_value, after_value)
            metric_instances.append(metric_instance)

        return metric_instances

    @staticmethod
    def build_partition_groups(partition_files_sizes: List[int],
                               max_bytes_per_group: int) -> List[List[int]]:
        """Builds groups of partition files based on size constraints."""
        sorted_sizes = sorted(partition_files_sizes)
        result, current_group = [], []
        current_size_bytes = 0

        for file_size in sorted_sizes:
            if current_size_bytes > max_bytes_per_group:
                result.append(current_group)
                current_group = []
                current_size_bytes = 0
            current_group.append(file_size)
            current_size_bytes += file_size

        if current_group:
            result.append(current_group)

        return result

    @staticmethod
    def calc_read_file_cost(bytes_size: int) -> int:
        """
        Calculate the cost of reading a file based on its size.

        Args:
            bytes_size (int): Size of the file in bytes.

        Returns:
            int: The calculated read file cost.
        """
        return bytes_size // FETCH_SIZE + 2

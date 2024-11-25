# Iceberg Diagnostics Tool

## Overview

The Iceberg Table Analysis CLI Tool evaluates Iceberg tables to identify how optimizations can enhance efficiency. 
It presents a side-by-side comparison of current metrics against potential improvements in scan durations, file counts,
and file sizes, providing a straightforward assessment of optimization opportunities.


## Installation
`iceberg-diag` can be used with [`uv`](https://docs.astral.sh/uv):

### Using uv
Clone the repository, install uv.

 ```bash
uv venv
source .venv/bin/activate
uv sync

uv run iceberg-diag [options]
 ```


## Usage Instructions

```bash
uv run iceberg-diag [options]
```

### Command-Line Options

- `-h`, `--help`: Display the help message and exit.
- `--catalog-uri CATALOG_URI`: Set the catalog URI. You should include the protocol and port (e.g., `thrift://1.2.3.4:9083`)
- `--region REGION`: Set the AWS region for operations, defaults to the specified profile's default region.
- `--database DATABASE`: Set the database name, will list all available iceberg tables if no `--table-name` provided.
- `--table-name TABLE_NAME`: Enter the table name or a glob pattern (e.g., `'*'`, `'tbl_*'`).
- `--file-target-size-mb TABLE_NAME`: Target size of files in MB.
- `--display-partitions True`: Wherever to display individual partitions stats.
- `--display-partitions-limit INT`: Limit of partitions stats to display.
- `--display-partitions-sort 'COLUMN NAME`: Column to sort partitions stats.
Provides more detailed analytics and includes information about file size reduction.
- `-v, --verbose`: Enable verbose logging

### Usage
1. Displaying help information:
    ```bash
     iceberg-diag --help
    ```
   
2. Listing all available databases in catalog:
    ```bash
   iceberg-diag --catalog-uri <catalog-uri>
    ```
   
3. Listing all available iceberg tables in a given database:
    ```bash
   iceberg-diag --catalog-uri <catalog-uri>
    ```
4. Running diagnostics on a specific table on specific region region (completely locally):
   ```bash
    iceberg-diag --catalog-uri <catalog-uri> --region <region> --database <database> --table-name '*'
   ```
   
5. Running diagnostic on a specific table and display individual partitions stats:
   ```bash
    iceberg-diag --catalog-uri <catalog-uri> --region <region> --database <database> --table-name '*'  --display-partitions True --display-partitions-limit 5 --display-partitions-sort 'Improvement Scan Overhead %'
   ```

   Output Example:
   
| Metric                        | Before    | After  | Improvement |
|-------------------------------|-----------|--------|-------------|
| Full Scan Overhead            | 6.65s     | 6.44s  | 3.20%       |
| Worst Partition Scan Overhead | 0.04s     | <0.01s | 88.89%      |
| Total File Count              | 1756      | 1649   | 6.09%       |
| Worst Partition File Count    | 18        | 2      | 94.44%      |
| Total Table Size              | 71.33 MB  |        |             |
| Total Partition Size          | 122.36 GB |        |             |
| Largest Partition Size        | 960.45 MB |        |             |
| Total Partitions              | 1649      |        |             |

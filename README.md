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
4. Running diagnostics on a specific table in a specific AWS profile and region (completely locally):
   ```bash
    iceberg-diag --catalog-uri <catalog-uri> --region <region> --database <database> --table-name '*'
    ```

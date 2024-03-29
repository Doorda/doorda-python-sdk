# Doorda Python SDK
[![GitHub Action](https://github.com/doorda/doorda-python-sdk/workflows/Unit%20Tests/badge.svg)](https://github.com/Doorda/doorda-python-sdk/actions?query=workflow%3A%22Unit+Tests%22)
[![GitHub Action](https://github.com/doorda/doorda-python-sdk/workflows/Upload%20Python%20Package/badge.svg)](https://github.com/Doorda/doorda-python-sdk/actions?query=workflow%3A%22Upload+Python+Package%22)
[![PyPI version](https://badge.fury.io/py/doorda-sdk.svg)](https://badge.fury.io/py/doorda-sdk)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/doorda-sdk)](https://pypi.python.org/pypi/doorda-sdk/)

## Requirements

- Python 3.6/3.7
- [DoordaHost Account](https://doorda.com)

## Installation

### PyPi
```bash
$ pip install doorda-sdk
```

## Usage

### DoordaHost

1) Connect to database
    ```python
    from doorda_sdk.host import client

    conn = client.connect(username="username",
                          password="password",
                          catalog="catalog_name",
                          schema="schema_name")
    cursor = conn.cursor()
    ```

2) Execute Queries
    ```python
    cursor.execute("SELECT * FROM table_name")

    # Returns generator of results
    # Does not put result into memory. Iterates through rows in a streaming fashion.
    # DEPRECATED
    for row in cursor.iter_result():
        # Do something with row
    # Use:
    for row in cursor:
        # Do something with row

    
    # Fetch all results
    rows = cursor.fetchall()
    
    # Fetch one results
    rows = cursor.fetchone()
    
    # Fetch multiple results
    rows = cursor.fetchmany(size=10)
    
    # Get list of column names
    cursor.col_names
    
    # Get column names mapped to data types
    cursor.col_types
    ```

3) Simplified Functions

    ```python
    # List Permissions
    ## Permissions are shown as a hierarchical tree structure
    ### Level 1 = Catalog, Level 2 = Schemas, Level 3 = Table Names

    permissions = cursor.permissions()
    print(permissions)

    # Check database connection
    results = cursor.is_connected()
    
    # List all catalogs
    rows = cursor.show_catalogs()

    # List all schemas
    rows = cursor.show_schemas("catalog_name")

    # List all tables
    rows = cursor.show_tables("catalog_name", "schema_name")
    
    # Get number of rows
    rows = cursor.table_stats(catalog="catalog_name", 
                              schema="schema_name",
                              table="table_name")
    ```

## Find out more

To find out more about DoordaHost, head over to [https://github.com/Doorda/Getting-Started](https://github.com/Doorda/Getting-Started/blob/master/README.md)

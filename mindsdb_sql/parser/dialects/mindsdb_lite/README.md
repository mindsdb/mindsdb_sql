# MindsDB Lite Minimal SQL processor

A minmial implementation of the core elements of MindsDB. The following SQL functions are implemented for only 
MindsDB objects:

MindsDB views are aliases for performing inference and inserting results back into native databases.
```
MINDSDB CREATE
    [OR REPLACE]
    VIEW view_name
    AS select_statement
```

MindsDB `DATASETs` are used to train models and represent Ray datasets in the cloud, or Pandas dataframes locally.
```
MINDSDB CREATE
    [OR REPLACE]
    DATASET dataset_name
    AS select_statement
```

Models are created with the following syntax:
```
CREATE MODEL model_name
FROM dataset_name
PREDICT target_column [, target_column] ...
```

`MINDSDB SELECT` statements have a number of use cases.
1. Operate on `MINDSDB DATASETS` by performing pandas/ray operations, to create new datasets.
2. Perform inference with a model based on inputs specified by the `WHERE` clause.
3. `JOIN` a model and a `MINDSDB VIEW` within the `FROM` clause, rendering predictions on a table.
```
MINDSDB SELECT select_expr [, select_expr] ...
    FROM {dataset_references | model | model_view_reference}
    [WHERE where_condition]
    [GROUP BY {col_name | expr | position}, ... [WITH ROLLUP]]
    [ORDER BY {col_name | expr | position} [ASC | DESC], ... [WITH ROLLUP]] 
    [LIMIT {[offset,] row_count | row_count OFFSET offset}]
    [INTO view_reference]
```
The `where_condition` can contain the `LATEST` keyword to denote the unprocessed rows in a `MINDSDB VIEW`.

`MINDSDB JOBS` map to Prefect tasks.

`MINDSDB PIPELINES` map to Prefect pipelines and are composed as jobs. 



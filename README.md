
# Installation


```
pip install mindsdb_sql
```

# Components


Parser. 
- Takes a string as input and parses it to AST-tree 

Planner
- Takes AST-tree as input and converts it to sequence of steps to perform query 

Render
- Takes AST-tree as input and converts it to sql string of selected dialect

# Parser


## How to use

```python

from mindsdb_sql import parse_sql

query = parse_sql('select b from aaa where c=1', dialect='mindsdb')

# result is abstract syntax tree (AST) 
query

# string representation of AST
query.to_tree()

# representation of tree as sql string. it can not exactly match with original sql
query.to_string()

```

## Available dialects

mysql
- Sql dialect of mysql-server. Is not complete and in process of improving  

sqlite 
- Not complete yet and is simplified version of the mysql syntax now

mindsdb
- Extended mysql dialect with support of mindsdb sql commands and operators [https://docs.mindsdb.com/]

## Architecture

### Parsing
For parsing is used [SLY](https://sly.readthedocs.io/en/latest/sly.html) library.

Parsing consists of 2 stages, (separate module for every dialect): 
- Defining keywords in lexer.py module. It is made mostly with regexp 
- Defining syntax rules in parser.py module. It is made by describing rules in [BNF grammar](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_form)
  - Syntax is defined in decorator of function. Inside of decorator you can use keyword itself or other function from parser
  - Output of function can be used as input in other functions of parser
  - Outputs of the parser is listed in "Top-level statements". It has to be Abstract syntax tree (AST) object.

SLY does not support inheritance, therefore every dialect is described completely, without extension one from another.  

### [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
- Structure of AST is defined in separate modules (in parser/ast/).
- It can be inherited
- Every class have to have these methods:
  - to_tree - to return hierarchical representation of object
  - get_string - to return object as sql expression (or sub-expression)
  - copy - to copy AST-tree to new object

# Planner


## How to use

**Initialize planner**

```python
from mindsdb_sql.planner import query_planner

# all parameters are optional
planner = query_planner.QueryPlanner(
    ast_query, # query as AST-tree
    integrations=['mysql'], # list of available integrations
    predictor_namespace='mindsdb', # name of namespace to lookup for predictors
    default_namespace='mindsdb', # if namespace is not set in query default namespace will be used
    predictor_metadata={ # information about predictors
        'tp3': { # name of predictor
           'timeseries': True, # is timeseries predictor
           'order_by_column': 'pickup_hour', # timeseries column 
           'group_by_columns': ['day', 'type'], # columns for partition (only for timeseries) 
           'window': 10 # windows size (only for timeseries) 
        }
    }
)

```
Detailed description of timeseries predictor: [https://docs.mindsdb.com/sql/create/predictor/]


**Plan of prepared statement**

Planner can be used in case of query with parameters: query is not complete and can't be executed. 
But it is possible to get list of columns and parameters from query.

```python
for step in planner.prepare_steps(ast_query):
    data = do_execute_step(step)
    step.set_result(data)

statement_info = planner.get_statement_info()

# list of columns
print(statement_info['columns'])

# list of parameters
print(statement_info['parameters'])
```

At the moment this functionality is used only in COM_STMT_PREPARE command of mysql binary protocol.

**Plan of execution**

```python

# if prepare_steps was executed we need pass params.
# otherwise, params=None
for step in planner.execute_steps(params):
    data = do_execute_step(step)
    step.set_result(data)
```

Query result data will be on output of the last step.

**Alternative way of execution**

At the moment execution plan doesn't dependent from results of previous steps. 
But this behavior can be changed in the future.

With the current behavior that it is possible to get plan of query as list:

```python
from mindsdb_sql.planner import plan_query
plan = plan_query(
    ast_query,
    integrations=['mysql'], 
    predictor_namespace='mindsdb', 
    default_namespace='mindsdb', 
    predictor_metadata={
        'tp3': {
           'timeseries': False, 
        }
    }
)
# list of steps
print(plan.steps)

```

## Architecture

Planner is analysing AST-query and return sequence of steps that is needed to execute to perform query.

Steps are defined in planner/steps.py. Steps can reference to future result of previous step (using class Result in planner/step_results.py)

Query planner consists from 2 different planner:

1. For prepare statement is class PreparedStatementPlanner in query_prepare.py

2. For execution is class QueryPlanner in query_panner.py
The most complex part of planner is planning of join table with timeseries predictor. Logic briefly:
- extract query for integration (without predictor)
- select all possible values of group fields (in scope of query)
- for every value of group field
  - select part of data according to filters and size of window
- join all data in one dataframe
- pass it to predictor input
- join predictor results with data before prediction 

**Useful functions** 

1. planner.utils.query_traversal

It can be used to analyse composition of AST-tree. An example:

```python
query_predictors = []
def find_predictors(node, is_table, **kwargs):
    if is_table and isinstance(node, ast.Identifier):
        if is_predictor(node):
            query_predictors.append(node)

utils.query_traversal(ast_query, find_predictors)
```

# Render

Renderer is using to convert AST-query to sql string using different sql dialects.

## How to use

```python
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

renderer = SqlalchemyRender('mysql') # select dialect
sql = renderer.get_string(ast_query, with_failback=True)

```
If with_failback==True: in case if sqlalchemy unable to render query 
string will be returned from sql representation of AST-tree (with method to_string) 


## Architecture

Only one renderer is available at the moment: SqlalchemyRender.
- It converts AST-query to sqlalchemy query. 
It uses [imperative](https://docs.sqlalchemy.org/en/14/orm/mapping_styles.html#orm-imperative-mapping) mapping for this 
- Then created sqlalchemy object is compiled inside sqlalchemy using chosen dialect 

Supported dialects at the moment: mysql, postgresql, sqlite, mssql, firebird, oracle, sybase

Notes:
- it is not possible to use more than 2 part in table name
  - it can be (integration.table) or (schema.table)
  - but can't be (integration.schema.table)
- sometimes conditions in rendered sql can be slightly changed, for example 'not a=b' to 'a!=b'


# How to test

It runs all tests for components 

```bash
env PYTHONPATH=./ pytest
```

import inspect
import pkgutil
import sys
import os
import importlib

from mindsdb_sql import parse_sql


def load_all_modules_from_dir(dir_names):
    for importer, package_name, _ in pkgutil.iter_modules(dir_names):
        full_package_name = package_name
        if full_package_name not in sys.modules:
            spec = importer.find_spec(package_name)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            yield module


def check_module(module):
    if module.__name__ in ('test_mysql_lexer', 'test_base_lexer'):
        # skip
        return

    for class_name, klass in inspect.getmembers(module, predicate=inspect.isclass):
        if not class_name.startswith('Test'):
            continue

        tests = klass()
        for test_name, test_method in inspect.getmembers(tests, predicate=inspect.ismethod):
            if not test_name.startswith('test_') or test_name.endswith('_error'):
                # skip tests that expected error
                continue
            sig = inspect.signature(test_method)
            args = []
            # add dialect
            if 'dialect' in sig.parameters:
                args.append('mindsdb')
            if 'cat' in sig.parameters:
                # skip it
                continue

            test_method(*args)


def parse_sql2(sql, dialect='mindsdb'):

    query = parse_sql(sql, dialect)

    # render
    sql2 = query.to_string()

    # Parse again
    try:
        query2 = parse_sql(sql2, dialect)
    except Exception as e:
        # TODO fix queries
        raise e
        print(sql2)
        return query

    # compare result from first and second parsing
    assert str(query) == str(query2)

    # return to test: it compares it with expected_ast
    return query2


def test_standard_render():

    base_dir = os.path.dirname(__file__)
    dir_names = [
        os.path.join(base_dir, folder)
        for folder in os.listdir(base_dir)
        if folder.startswith('test_')
    ]

    for module in load_all_modules_from_dir(dir_names):

        # inject function
        module.parse_sql = parse_sql2

        check_module(module)



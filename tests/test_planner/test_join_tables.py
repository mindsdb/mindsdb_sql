import pytest

from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.parser.ast import *
from mindsdb_sql.planner import plan_query
from mindsdb_sql.planner.query_plan import QueryPlan
from mindsdb_sql.planner.step_result import Result
from mindsdb_sql.planner.steps import (
    FetchDataframeStep, ProjectStep, FilterStep, JoinStep, GroupByStep,
    LimitOffsetStep, OrderByStep, ApplyPredictorStep, SubSelectStep, MapReduceStep,
    ApplyTimeseriesPredictorStep
)
from mindsdb_sql.parser.utils import JoinType
from mindsdb_sql import parse_sql

class TestPlanJoinTables:
    def test_join_tables_plan(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int2.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       )
                )
        plan = plan_query(query, integrations=['int', 'int2'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int2',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      ProjectStep(dataframe=Result(2),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
        )

        assert plan.steps == expected_plan.steps
        

    def test_join_tables_where_plan(self):
        query = parse_sql('''
          SELECT tab1.column1, tab2.column1, tab2.column2 
          FROM int.tab1 
          INNER JOIN int2.tab2 ON tab1.column1 = tab2.column1 
          WHERE ((tab1.column1 = 1) 
            AND (tab2.column1 = 0))
            AND (tab1.column3 = tab2.column3)
        ''')

        plan = plan_query(query, integrations=['int', 'int2'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=parse_sql('SELECT * FROM tab1 WHERE (column1 = 1)')),
                                      FetchDataframeStep(integration='int2',
                                                         query=parse_sql('SELECT * FROM tab2 WHERE (column1 = 0)')),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      FilterStep(dataframe=Result(2),
                                                 query=BinaryOperation('and',
                                                                       args=[
                                                                           BinaryOperation('and', parentheses=True,
                                                                                           args=[
                                                                                               BinaryOperation('=', parentheses=True,
                                                                                                               args=[
                                                                                                                   Identifier(
                                                                                                                       'tab1.column1'),
                                                                                                                   Constant(
                                                                                                                       1)]),
                                                                                               BinaryOperation('=', parentheses=True,
                                                                                                               args=[
                                                                                                                   Identifier(
                                                                                                                       'tab2.column1'),
                                                                                                                   Constant(
                                                                                                                       0)]),

                                                                                           ]
                                                                                           ),
                                                                           BinaryOperation('=', parentheses=True,
                                                                                           args=[Identifier(
                                                                                               'tab1.column3'),
                                                                                                 Identifier(
                                                                                                     'tab2.column3')]),
                                                                       ]
                                                                       )),
                                      ProjectStep(dataframe=Result(3),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
                                  )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]


    def test_join_tables_plan_groupby(self):
        query = Select(targets=[
            Identifier('tab1.column1'),
            Identifier('tab2.column1'),
            Function('sum', args=[Identifier('tab2.column2')], alias=Identifier('total'))],
            from_table=Join(left=Identifier('int.tab1'),
                            right=Identifier('int2.tab2'),
                            condition=BinaryOperation(op='=',
                                                      args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                            join_type=JoinType.INNER_JOIN
                            ),
            group_by=[Identifier('tab1.column1'), Identifier('tab2.column1')],
            having=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Constant(0)])
        )
        plan = plan_query(query, integrations=['int', 'int2'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int2',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      GroupByStep(dataframe=Result(2),
                                                  targets=[Identifier('tab1.column1'),
                                                            Identifier('tab2.column1'),
                                                            Function('sum', args=[Identifier('tab2.column2')])],
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                      FilterStep(dataframe=Result(3), query=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Constant(0)])),
                                      ProjectStep(dataframe=Result(4),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'),
                                                           Function(op='sum', args=[Identifier('tab2.column2')], alias=Identifier('total'))]),
                                  ],
                                  )
        assert plan.steps == expected_plan.steps
        

    def test_join_tables_plan_limit_offset(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int2.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       ),
                       limit=Constant(10),
                       offset=Constant(15),
                )
        plan = plan_query(query, integrations=['int', 'int2'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int2',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      LimitOffsetStep(dataframe=Result(2), limit=10, offset=15),
                                      ProjectStep(dataframe=Result(3),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
                                  )

        assert plan.steps == expected_plan.steps
        

    def test_join_tables_plan_order_by(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int2.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'), Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       ),
                       limit=Constant(10),
                       offset=Constant(15),
                       order_by=[OrderBy(field=Identifier('tab1.column1'))],
                )
        plan = plan_query(query, integrations=['int', 'int2'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=Select(
                                                             targets=[Star()],
                                                             from_table=Identifier('tab1')),
                                                         ),
                                      FetchDataframeStep(integration='int2',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'),
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      OrderByStep(dataframe=Result(2), order_by=[OrderBy(field=Identifier('tab1.column1'))]),
                                      LimitOffsetStep(dataframe=Result(3), limit=10, offset=15),
                                      ProjectStep(dataframe=Result(4),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
                                  )

        assert plan.steps == expected_plan.steps
        

    def test_join_tables_where_ambigous_column_error(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'),
                                                                               Identifier('tab2.column1')]),
                                       join_type=JoinType.INNER_JOIN
                                       ),
                       where=BinaryOperation('and',
                                             args=[
                                                 BinaryOperation('and',
                                                                 args=[
                                                                     BinaryOperation('=',
                                                                                     args=[Identifier('tab1.column1'),
                                                                                           Constant(1)]),
                                                                     BinaryOperation('=',
                                                                                     args=[Identifier('tab2.column1'),
                                                                                           Constant(0)]),

                                                                 ]
                                                                 ),
                                                 BinaryOperation('=',
                                                                 args=[Identifier('column3'),
                                                                       Constant(0)]),
                                                 # Ambigous column: no idea what table column3 comes from
                                             ]
                                             )
                       )

        with pytest.raises(PlanningException) as e:
            plan_query(query, integrations=['int'])

    def test_join_tables_disambiguate_identifiers_in_condition(self):
        query = parse_sql('''
            SELECT tab1.column1, tab2.column1, tab2.column2 
            FROM int.tab1 
            INNER JOIN int.tab2 ON int.tab1.column1 = tab2.column1
        ''')
        plan = plan_query(query, integrations=['int'])
        expected_plan = QueryPlan(integrations=['int'],
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=query),
                                      FetchDataframeStep(integration='int',
                                                         query=Select(targets=[Star()],
                                                                      from_table=Identifier('tab2')),
                                                         ),
                                      JoinStep(left=Result(0), right=Result(1),
                                               query=Join(left=Identifier('tab1'),
                                                          right=Identifier('tab2'),
                                                          condition=BinaryOperation(op='=',
                                                                                    args=[Identifier('tab1.column1'), # integration name gets stripped out
                                                                                          Identifier('tab2.column1')]),
                                                          join_type=JoinType.INNER_JOIN
                                                          )),
                                      ProjectStep(dataframe=Result(2),
                                                  columns=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')]),
                                  ],
                                  )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]
        

    def _disabled_test_join_tables_error_on_unspecified_table_in_condition(self):
        # disabled: identifier can be environment of system variable
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'),
                                                                               Identifier('column1')]), #Table name omitted
                                       join_type=JoinType.INNER_JOIN
                                       ))
        with pytest.raises(PlanningException):
            plan_query(query, integrations=['int'])

    def test_join_tables_error_on_wrong_table_in_condition(self):
        query = Select(targets=[Identifier('tab1.column1'), Identifier('tab2.column1'), Identifier('tab2.column2')],
                       from_table=Join(left=Identifier('int.tab1'),
                                       right=Identifier('int2.tab2'),
                                       condition=BinaryOperation(op='=', args=[Identifier('tab1.column1'),
                                                                               Identifier('tab3.column1')]), #Wrong table name
                                       join_type=JoinType.INNER_JOIN
                                       ))
        with pytest.raises(PlanningException) as e:
            plan_query(query, integrations=['int', 'int2'])

    def test_join_tables_plan_default_namespace(self):
        query = parse_sql('''
          SELECT tab1.column1, tab2.column1, tab2.column2
           FROM tab1 
           INNER JOIN tab2 ON tab1.column1 = tab2.column1
        ''')

        expected_plan = QueryPlan(integrations=['int'],
                                  default_namespace='int',
                                  steps = [
                                      FetchDataframeStep(integration='int',
                                                         query=parse_sql('''
                                                             SELECT tab1.column1, tab2.column1, tab2.column2 
                                                             FROM tab1 
                                                             INNER JOIN tab2 ON tab1.column1 = tab2.column1
                                                         ''')),
                                  ],
        )
        plan = plan_query(query, integrations=['int'], default_namespace='int')

        assert plan.steps == expected_plan.steps

    def test_complex_join_tables(self):
        query = parse_sql('''
            select * from int1.tbl1 t1 
            right join int2.tbl2 t2 on t1.id=t2.id
            join pred m
            left join tbl3 on tbl3.id=t1.id
            where t1.a=1 and t2.b=2 and 1=1
        ''', dialect='mindsdb')

        plan = plan_query(query, integrations=['int1', 'int2', 'proj'],  default_namespace='proj',
                          predictor_metadata=[{'name': 'pred', 'integration_name': 'proj'}])

        expected_plan = QueryPlan(
            steps=[
              FetchDataframeStep(integration='int1', query=parse_sql('select * from tbl1 as t1 where a=1')),
              FetchDataframeStep(integration='int2', query=parse_sql('select * from tbl2 as t2 where b=2')),
              JoinStep(left=Result(0),
                       right=Result(1),
                       query=Join(left=Identifier('tab1'),
                                  right=Identifier('tab2'),
                                  condition=BinaryOperation(
                                      op='=',
                                      args=[Identifier('t1.id'),
                                            Identifier('t2.id')]),
                                  join_type=JoinType.RIGHT_JOIN)),
              ApplyPredictorStep(namespace='proj', dataframe=Result(2), predictor=Identifier('pred', alias=Identifier('m'))),
              JoinStep(left=Result(2),
                       right=Result(3),
                       query=Join(left=Identifier('tab1'),
                                    right=Identifier('tab2'),
                                    join_type=JoinType.JOIN)),
              FetchDataframeStep(integration='proj', query=parse_sql('select * from tbl3')),
              JoinStep(left=Result(4),
                         right=Result(5),
                         query=Join(left=Identifier('tab1'),
                                    right=Identifier('tab2'),
                                    condition=BinaryOperation(
                                        op='=',
                                        args=[Identifier('tbl3.id'),
                                              Identifier('t1.id')]),
                                    join_type=JoinType.LEFT_JOIN)),
              FilterStep(dataframe=Result(6),
                         query=BinaryOperation(op='and',
                              args=(
                                BinaryOperation(op='and',
                                  args=(
                                    BinaryOperation(op='=',
                                      args=(
                                        Identifier(parts=['t1', 'a']),
                                        Constant(value=1)
                                      )
                                    ),
                                    BinaryOperation(op='=',
                                      args=(
                                        Identifier(parts=['t2', 'b']),
                                        Constant(value=2)
                                      )
                                    )
                                  )
                                ),
                                BinaryOperation(op='=',
                                  args=(
                                    Constant(value=1),
                                    Constant(value=1)
                                  )
                                )
                              )
                            )
              ),
              ProjectStep(dataframe=Result(7), columns=[Star()])
            ]
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_complex_join_tables_subselect(self):
        query = parse_sql('''
            select * from int1.tbl1 t1 
            join (
                select * from int2.tbl3
                join pred m
            ) t2 on t1.id = t2.id
        ''', dialect='mindsdb')

        plan = plan_query(query, integrations=['int1', 'int2', 'proj'],  default_namespace='proj',
                          predictor_metadata=[{'name': 'pred', 'integration_name': 'proj'}])

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int1', query=parse_sql('select * from tbl1 as t1')),
                FetchDataframeStep(integration='int2', query=parse_sql('select * from tbl3')),
                ApplyPredictorStep(namespace='proj', dataframe=Result(1),
                                   predictor=Identifier('pred', alias=Identifier('m'))),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(left=Identifier('result_1'),
                                    right=Identifier('result_2'),
                                    join_type=JoinType.JOIN)),
                SubSelectStep(dataframe=Result(3), query=Select(targets=[Star()]), table_name='t2'),
                JoinStep(
                     left=Result(0),
                     right=Result(4),
                     query=Join(
                        left=Identifier('tab1'),
                        right=Identifier('tab2'),
                        join_type=JoinType.JOIN,
                        condition=BinaryOperation(
                             op='=',
                             args=[Identifier('t1.id'),
                                   Identifier('t2.id')])
                     )
                ),
            ]
        )

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_join_with_select_from_native_query(self):
        query = parse_sql('''
            select * from (
                select * from int1 (
                    select raw query
                )
            ) t1 
            join pred m          
        ''', dialect='mindsdb')

        plan = plan_query(query, integrations=['int1', 'int2', 'proj'],  default_namespace='proj',
                          predictor_metadata=[{'name': 'pred', 'integration_name': 'proj'}])

        expected_plan = QueryPlan(
            steps=[
                FetchDataframeStep(integration='int1', raw_query='select raw query'),
                SubSelectStep(step_num=1, references=[], query=Select(targets=[Star()]), dataframe=Result(0), table_name='t1'),
                ApplyPredictorStep(namespace='proj', dataframe=Result(1),
                                   predictor=Identifier('pred', alias=Identifier('m'))),
                JoinStep(left=Result(1),
                         right=Result(2),
                         query=Join(left=Identifier('tab1'),
                                    right=Identifier('tab2'),
                                    join_type=JoinType.JOIN)),
            ]
        )

        assert len(plan.steps) == len(expected_plan.steps)
        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

        # select from native query
        # has the same plan

        query = parse_sql('''
            select * from int1 (
                select raw query
            ) t1 
            join pred m          
        ''', dialect='mindsdb')

        plan = plan_query(query, integrations=['int1', 'int2', 'proj'], default_namespace='proj',
                          predictor_metadata=[{'name': 'pred', 'integration_name': 'proj'}])

        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

    def test_join_one_integration(self):
        query = parse_sql('''
          SELECT tab1.column1
           FROM int.tab1 
           JOIN tab2 ON tab1.column1 = tab2.column1
        ''')

        expected_plan = QueryPlan(integrations=['int'],
                                  default_namespace='int',
                                  steps=[
                                      FetchDataframeStep(integration='int',
                                                         query=parse_sql('''
                                                             SELECT tab1.column1
                                                             FROM tab1 
                                                             JOIN tab2 ON tab1.column1 = tab2.column1
                                                         ''')),
                                  ],
                                  )
        plan = plan_query(query, integrations=['int'], default_namespace='int')

        assert plan.steps == expected_plan.steps

    def test_join_native_query(self):
        query = parse_sql('''
            SELECT *
            FROM int1 (select * from tab) as t
            JOIN pred as m
            WHERE t.date > LATEST
        ''')

        group_by_column = 'type'

        plan = plan_query(
            query,
            integrations=['int1'],
            default_namespace='proj',
            predictor_metadata=[{
                'name': 'pred',
                'integration_name': 'proj',
                'timeseries': True,
                'window': 10, 'horizon': 10, 'order_by_column': 'date', 'group_by_columns': [group_by_column]
            }]
        )

        expected_plan = QueryPlan(steps=[
            FetchDataframeStep(
                integration='int1',
                query=Select(
                    targets=[Identifier('t.type', alias=Identifier('type'))],
                    from_table=StrQuery(query='select * from tab', alias=Identifier('t')),
                    distinct=True
                )
            ),
            MapReduceStep(
                values=Result(0),
                reduce='union',
                step=FetchDataframeStep(integration='int1',
                    query=Select(
                        targets=[Star()],
                        from_table=StrQuery(query='select * from tab', alias=Identifier('t')),
                        distinct=False,
                        limit=Constant(10),
                        order_by=[OrderBy(field=Identifier('t.date'), direction='DESC')],
                        where=BinaryOperation('and', args=[
                            BinaryOperation('is not', args=[Identifier('t.date'), NullConstant()]),
                            BinaryOperation('=', args=[Identifier('t.type'), Constant('$var[type]')]),
                        ])
                    )
                ),
            ),
            ApplyTimeseriesPredictorStep(
                namespace='proj',
                predictor=Identifier('pred', alias=Identifier('m')),
                dataframe=Result(1),
                output_time_filter=BinaryOperation('>', args=[Identifier('t.date'), Latest()]),
            ),
            JoinStep(
                left=Result(1),
                right=Result(2),
                query=Join(
                    left=Identifier('result_1'),
                    right=Identifier('result_2'),
                    join_type=JoinType.JOIN
                )
            )
        ])

        assert len(plan.steps) == len(expected_plan.steps)
        for i in range(len(plan.steps)):
            assert plan.steps[i] == expected_plan.steps[i]

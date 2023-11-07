from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *


class TestSkills:
    def test_create_skill(self):
        sql = '''
            create skill if not exists my_skill
            using 
            type = 'knowledge_base', 
            source ='my_knowledge_base'
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateSkill(
            name=Identifier('my_skill'),
            type='knowledge_base',
            params={'source': 'my_knowledge_base'},
            if_not_exists=True
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()
        
        # Parse again after rendering to catch problems with rendering.
        ast = parse_sql(str(ast), dialect='mindsdb')
        assert str(ast) == str(expected_ast)

    def test_update_skill(self):
        sql = '''
            update skill my_skill
            set
            source = 'new_source'
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_params = {
            'source': 'new_source'
        }
        expected_ast = UpdateSkill(
            name=Identifier('my_skill'),
            updated_params=expected_params
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # Parse again after rendering to catch problems with rendering.
        ast = parse_sql(str(ast), dialect='mindsdb')
        assert str(ast) == str(expected_ast)

    def test_drop_skill(self):
        sql = '''
            drop skill if exists my_skill
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = DropSkill(
            name=Identifier('my_skill'), if_exists=True
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

        # Parse again after rendering to catch problems with rendering.
        ast = parse_sql(str(ast), dialect='mindsdb')
        assert str(ast) == str(expected_ast)

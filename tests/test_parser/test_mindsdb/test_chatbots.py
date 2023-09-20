from mindsdb_sql import parse_sql
from mindsdb_sql.parser.dialects.mindsdb import *
from mindsdb_sql.parser.ast import *


class TestChatbots:
    def test_test_create_chatbot(self):
        sql = '''
            create chatbot mybot
            using 
            model = 'chat_model', 
            database ='my_rocket_chat',
            agent = 'my_agent'
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateChatBot(
            name=Identifier('mybot'),
            database=Identifier('my_rocket_chat'),
            model=Identifier('chat_model'),
            agent=Identifier('my_agent')
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_test_create_chatbot_with_params(self):
        sql = '''
            create chatbot mybot
            using 
            model = 'chat_model', 
            database ='my_rocket_chat',
            key = 'value'
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = CreateChatBot(
            name=Identifier('mybot'),
            database=Identifier('my_rocket_chat'),
            model=Identifier('chat_model'),
            agent=NullConstant(),
            params={'key': 'value'}
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_update_chatbot(self):
        sql = '''
            update chatbot mybot
            set
            name = 'new_name',
            model = 'new_model',
            database = 'new_database',
            chat_engine = 'new_chat_engine',
            is_running = true,
            new_param = 'new_value'
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_params = {
            'name': 'new_name',
            'model': 'new_model',
            'database': 'new_database',
            'chat_engine': 'new_chat_engine',
            'is_running': True,
            'new_param': 'new_value'
        }
        expected_ast = UpdateChatBot(
            name=Identifier('mybot'),
            updated_params=expected_params
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

    def test_drop_chatbot(self):
        sql = '''
            drop chatbot mybot
        '''
        ast = parse_sql(sql, dialect='mindsdb')
        expected_ast = DropChatBot(
            name=Identifier('mybot'),
        )
        assert str(ast) == str(expected_ast)
        assert ast.to_tree() == expected_ast.to_tree()

import pandas as pd
from sqlalchemy import create_engine


class BaseDBConnection:
    def __init__(self, params, *args, **kwargs):
        self.params = params

    def query(self, query):
        pass


class SQLConnection(BaseDBConnection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = create_engine(self.params['db_url'],
                                    echo=self.params['db_echo'])
        self.connection = self.engine.connect().execution_options(autocommit=False)

    def query_raw(self, query):
        return self.connection.execute(query)

    def query(self, query):
        result = self.query_raw(query)
        rows = list(result.all())
        columns = result.keys()
        df = pd.DataFrame(rows, columns=columns)
        return df


class MongoConnection(BaseDBConnection):
    pass

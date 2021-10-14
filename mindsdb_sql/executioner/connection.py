class BaseDBConnection:
    def __init__(self, params):
        self.params = params

    def get_connection_string(self):
        pass

    def is_alive(self):
        pass

    def query(self, query):
        pass


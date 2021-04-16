from dfsql.sql_parser.base import Statement


LOOKUP_JOIN_TYPE = {
    0: 'INNER JOIN',
    1: 'LEFT JOIN',
    2: 'FULL JOIN',
    3: 'RIGHT JOIN'
}


class Join(Statement):
    def __init__(self, join_type, left, right, condition, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.join_type = join_type
        self.left = left
        self.right = right
        self.condition = condition

    def to_string(self, *args, **kwargs):
        return f'{self.left.to_string()} {self.join_type} {self.right.to_string()} ON {self.condition.to_string()}'

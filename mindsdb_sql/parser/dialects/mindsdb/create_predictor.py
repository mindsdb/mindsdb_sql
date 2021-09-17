import json
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.utils import indent


class CreatePredictor(ASTNode):
    def __init__(self,
                 name,
                 integration_name,
                 query,
                 datasource_name,
                 targets,
                 order_by=None,
                 group_by=None,
                 window=None,
                 horizon=None,
                 using=None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name
        self.integration_name = integration_name
        self.query = query
        self.datasource_name = datasource_name
        self.targets = targets
        self.order_by = order_by
        self.group_by = group_by
        self.window = window
        self.horizon = horizon
        self.using = using

    def to_tree(self, *args, level=0, **kwargs):
        ind = indent(level)
        ind1 = indent(level+1)

        name_str = f'\n{ind1}name={repr(self.name)},'
        integration_name_str = f'\n{ind1}integration_name={repr(self.integration_name)},'
        query_str = f'\n{ind1}query={repr(self.query)},'
        datasource_name_str = f'\n{ind1}datasource_name={repr(self.datasource_name)},'

        target_trees = ',\n'.join([t.to_tree(level=level+2) for t in self.targets])
        targets_str = f'\n{ind1}targets=[\n{target_trees}\n{ind1}],'

        group_by_str = ''
        if self.group_by:
            group_by_trees = ',\n'.join([t.to_tree(level=level+2) for t in self.group_by])
            group_by_str = f'\n{ind1}group_by=[\n{group_by_trees}\n{ind1}],'

        order_by_str = ''
        if self.order_by:
            order_by_trees = ',\n'.join([t.to_tree(level=level + 2) for t in self.order_by])
            order_by_str = f'\n{ind1}order_by=[\n{order_by_trees}\n{ind1}],'

        window_str = f'\n{ind1}window={repr(self.window)},'
        horizon_str = f'\n{ind1}horizon={repr(self.horizon)},'
        using_str = f'\n{ind1}using={repr(self.using)},'

        out_str = f'{ind}CreatePredictor(' \
                  f'{name_str}' \
                  f'{integration_name_str}' \
                  f'{query_str}' \
                  f'{datasource_name_str}' \
                  f'{targets_str}' \
                  f'{order_by_str}' \
                  f'{group_by_str}' \
                  f'{window_str}' \
                  f'{horizon_str}' \
                  f'{using_str}' \
                  f'\n{ind})'
        return out_str

    def get_string(self, *args, **kwargs):
        targets_str = ', '.join([out.to_string() for out in self.targets])
        order_by_str = f'ORDER BY {", ".join([out.to_string() for out in self.order_by])} ' if self.order_by else ''
        group_by_str = f'GROUP BY {", ".join([out.to_string() for out in self.group_by])} ' if self.group_by else ''
        window_str = f'WINDOW {self.window} ' if self.window is not None else ''
        horizon_str = f'HORIZON {self.horizon} ' if self.horizon is not None else ''
        using_str = f'USING \'{json.dumps(self.using)}\'' if self.using is not None else ''
        out_str = f'CREATE PREDICTOR {str(self.name)} FROM {str(self.integration_name)} WITH ({repr(self.query)}) AS {str(self.datasource_name)} ' \
                  f'PREDICT {targets_str} ' \
                  f'{order_by_str}' \
                  f'{group_by_str}' \
                  f'{window_str}' \
                  f'{horizon_str}' \
                  f'{using_str}'

        return out_str.strip()

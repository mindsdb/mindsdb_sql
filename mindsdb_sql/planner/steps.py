from mindsdb_sql.exceptions import PlanningException
from mindsdb_sql.planner.step_result import Result


class PlanStep:
    def __init__(self, step_num=None, references=None):
        self.step_num = step_num
        self.references = references or []

    @property
    def result(self):
        if self.step_num is None:
            raise PlanningException(f'Can\'t reference a step with no assigned step number. Tried to reference: {type(self)}')
        return Result(self.step_num)

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        for k in vars(self):
            if getattr(self, k) != getattr(other, k):
                return False

        return True

    def __repr__(self):
        attrs_dict = vars(self)
        attrs_str = ', '.join([f'{k}={str(v)}' for k, v in attrs_dict.items()])
        return f'{self.__class__.__name__}({attrs_str})'


class ProjectStep(PlanStep):
    """Selects columns from a dataframe"""
    def __init__(self, columns, dataframe, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = columns
        self.dataframe = dataframe

        if isinstance(dataframe, Result):
            self.references.append(dataframe)


class FilterStep(PlanStep):
    """Filters some dataframe according to a query"""
    def __init__(self, dataframe, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.query = query

        if isinstance(dataframe, Result):
            self.references.append(dataframe)


class GroupByStep(PlanStep):
    """Groups output by columns and computes aggregation functions"""

    def __init__(self, dataframe, columns, targets, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.columns = columns
        self.targets = targets

        if isinstance(dataframe, Result):
            self.references.append(dataframe)


class JoinStep(PlanStep):
    """Joins two dataframes, producing a new dataframe"""
    def __init__(self, left, right, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right
        self.query = query

        if isinstance(left, Result):
            self.references.append(left)

        if isinstance(right, Result):
            self.references.append(right)


class UnionStep(PlanStep):
    """Union of two dataframes, producing a new dataframe"""
    def __init__(self, left, right, unique, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right
        self.unique = unique

        if isinstance(left, Result):
            self.references.append(left)

        if isinstance(right, Result):
            self.references.append(right)


class OrderByStep(PlanStep):
    """Applies sorting to a dataframe"""

    def __init__(self, dataframe, order_by, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.order_by = order_by

        if isinstance(dataframe, Result):
            self.references.append(dataframe)


class LimitOffsetStep(PlanStep):
    """Applies limit and offset to a dataframe"""
    def __init__(self, dataframe, limit=None, offset=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.limit = limit
        self.offset = offset

        if isinstance(dataframe, Result):
            self.references.append(dataframe)


class FetchDataframeStep(PlanStep):
    """Fetches a dataframe from external integration"""
    def __init__(self, integration, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.integration = integration
        self.query = query


class ApplyPredictorStep(PlanStep):
    """Applies a mindsdb predictor on some dataframe and returns a new dataframe with predictions"""
    def __init__(self, namespace, predictor, dataframe,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace = namespace
        self.predictor = predictor
        self.dataframe = dataframe

        if isinstance(dataframe, Result):
            self.references.append(dataframe)


class ApplyTimeseriesPredictorStep(ApplyPredictorStep):
    """Applies a mindsdb predictor on some dataframe and returns a new dataframe with predictions.
    Accepts an additional parameter output_time_filter that specifies for which dates the predictions should be returned
    """

    def __init__(self, *args, output_time_filter=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_time_filter = output_time_filter


class ApplyPredictorRowStep(PlanStep):
    """Applies a mindsdb predictor to one row of values and returns a dataframe of one row, the predictor."""
    def __init__(self, namespace, predictor, row_dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace = namespace
        self.predictor = predictor
        self.row_dict = row_dict


class GetPredictorColumns(PlanStep):
    """Returns an empty dataframe of shape and columns like predictor results."""
    def __init__(self, namespace, predictor, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace = namespace
        self.predictor = predictor


class MapReduceStep(PlanStep):
    """Applies a step for each value in a list, and then reduces results to a single dataframe"""
    def __init__(self, values, step, reduce, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.values = values
        self.step = step
        self.reduce = reduce

        if isinstance(values, Result):
            self.references.append(values)


class MultipleSteps(PlanStep):
    def __init__(self, steps, reduce, *args, **kwargs):
        """Runs multiple steps and reduces results to a single dataframe"""
        super().__init__(*args, **kwargs)
        self.steps = steps
        self.reduce = reduce



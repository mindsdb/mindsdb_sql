class PlanStep:
    def __eq__(self, other):
        if type(self) != type(other):
            return False

        for k in vars(self):
            if getattr(self, k) != getattr(other, k):
                return False

        return True


class ProjectStep(PlanStep):
    """Selects columns from a dataframe"""
    def __init__(self, columns, dataframe, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns = columns
        self.dataframe = dataframe


class FilterStep(PlanStep):
    """Filters some dataframe according to a query"""
    def __init__(self, dataframe, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.query = query


class GroupByStep(PlanStep):
    """Groups output by columns and computes aggregation functions"""

    def __init__(self, dataframe, columns, targets, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.columns = columns
        self.targets = targets


class JoinStep(PlanStep):
    """Joins two dataframes, producing a new dataframe"""
    def __init__(self, left, right, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right
        self.query = query


class UnionStep(PlanStep):
    """Union of two dataframes, producing a new dataframe"""
    def __init__(self, left, right, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right


class OrderByStep(PlanStep):
    """Applies sorting to a dataframe"""

    def __init__(self, dataframe, order_by, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.order_by = order_by


class LimitOffsetStep(PlanStep):
    """Applies limit and offset to a dataframe"""
    def __init__(self, dataframe, limit=None, offset=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.limit = limit
        self.offset = offset


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



class ApplyPredictorRowStep(PlanStep):
    """Applies a mindsdb predictor to one row of values and returns a dataframe of one row, the predictor."""
    def __init__(self, namespace, predictor, row_dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.namespace = namespace
        self.predictor = predictor
        self.row_dict = row_dict




class PlanStep:
    def __eq__(self, other):
        if isinstance(other, PlanStep):
            return type(self) == type(other) and all([getattr(self, k) == getattr(other, k) for k in self.__dict__])
        return False


class ProjectStep(PlanStep):
    """Selects columns from a dataframe"""
    def __init__(self, dataframe, columns, aliases=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.columns = columns
        self.aliases = aliases or {}


class JoinStep(PlanStep):
    """Joins two dataframes, producing a new dataframe"""
    def __init__(self, left, right, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.left = left
        self.right = right
        self.query = query


class FetchDataframeStep(PlanStep):
    """Fetches a dataframe from external integration"""
    def __init__(self, integration, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.integration = integration
        self.query = query


class ApplyPredictorStep(PlanStep):
    """Applies a mindsdb predictor on some dataframe and returns a new dataframe with predictions"""
    def __init__(self, dataframe, predictor, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.predictor = predictor


class ApplyPredictorRowStep(PlanStep):
    """Applies a mindsdb predictor to one row of values and returns a dataframe of one row, the predictor."""
    def __init__(self, predictor, row_dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.predictor = predictor
        self.row_dict = row_dict

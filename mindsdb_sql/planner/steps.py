class PlanStep:
    def __init__(self, save=False):
        self.save = save

    def __eq__(self, other):
        if isinstance(other, PlanStep):
            return self.__dict__ == other.__dict__
        return False


class ProjectStep(PlanStep):
    """Selects columns from a dataframe"""
    def __init__(self, dataframe, columns, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.columns = columns


class JoinStep(PlanStep):
    """Joins two dataframes, producing a new dataframe"""
    def __init__(self, dataframe_left, dataframe_right, join_type, condition, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe_left = dataframe_left
        self.dataframe_right = dataframe_right
        self.join_type = join_type
        self.condition = condition


class FetchDataframeStep(PlanStep):
    """Fetches a dataframe from external integration"""
    def __init__(self, integration, table, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.integration = integration
        self.table = table
        self.query = query


class ApplyPredictorStep(PlanStep):
    """Applies a mindsdb predictor on some dataframe and returns a new dataframe with predictions"""
    def __init__(self, dataframe, predictor, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dataframe = dataframe
        self.predictor = predictor

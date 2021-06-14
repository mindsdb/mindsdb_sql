class Result:
    """A placeholder for cached results of some previous plan step"""
    def __init__(self, step_num):
        self.step_num = step_num

    def __eq__(self, other):
        if isinstance(other, Result):
            return self.step_num == other.step_num
        return False


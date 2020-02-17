class CalculationNode(object):
    """
    Base implementation of all calculations.
    """
    def __init__(self):
        self.arguments = {}
        self.date_field = None

    def get_data(self, **kwargs):
        raise NotImplementedError('get_data is not implemented for class: {}'.format(self.__class__.__name__))

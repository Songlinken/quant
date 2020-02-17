class DataModel(object):
    """
    Base implementation of all data models.
    """
    def __init__(self):
        self.arguments = {}
        self.date_field = None

    def get_query(self):
        raise NotImplementedError('get_query is not implemented for class: {}'.format(self.__class__.__name__))

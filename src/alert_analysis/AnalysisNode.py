class AnalysisNode(object):
    """
    Base implementation of all analysis class.
    """
    def __init__(self):
        self.arguments = {}
        self.date_field = None

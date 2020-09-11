from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class MetalHistoryDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize with date field.
        """
        self.date_field = 'event_date'
        self.arguments = kwargs

        return self

    def get_query(self):

        date_condition = data_model_date_range(self)

        if self.arguments.get('other_condition', None):
            other_condition = self.arguments.get('other_condition')

        else:
            other_condition = '1=1'

        query = """
                select   event_date,
                         metal,
                         unit,
                         close_price
                from     metal_daily_data
                where    {date_condition}
                and      {other_condition};
                """.format(date_condition=date_condition, other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'mktsrv', keyword_arguments=self.arguments, ssh=None, port=5432)

        return data_frame

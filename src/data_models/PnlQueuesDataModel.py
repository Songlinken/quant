from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class PnlQueuesDataModel(DataModel):

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
                with    temp_data as 
                            (select       max(event_id) as event_id
                             from         ms_dev.pnl_data
                             where        {date_condition}
                             and          {other_condition}
                             group by     account_id, order_book)
                select  account_id,
                        order_book,
                        queues,
                        pd.event_id
                from    ms_dev.pnl_data as pd,
                        temp_data as td
                where   pd.event_id = td.event_id;
                """.format(date_condition=date_condition, other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

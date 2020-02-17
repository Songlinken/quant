from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class SmartsDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize with date field.
        """
        self.date_field = 'data_from_date'
        self.arguments = kwargs

        return self

    def get_query(self):
        date_condition = data_model_date_range(self)

        if self.arguments.get('other_condition', None):
            other_condition = self.arguments.get('other_condition')

        else:
            other_condition = '1=1'

        query = """
                select   account_id,
                         auction_id,
                         event_id,
                         event_date,
                         event_time,
                         event_millis,
                         event_type,
                         execution_options,
                         order_id,
                         order_type,
                         side,
                         symbol,
                         limit_price,
                         original_quantity_crypto,
                         gross_notional_value,
                         fill_price,
                         fill_quantity_crypto,
                         total_exec_quantity_crypto,
                         remaining_quantity_crypto,
                         avg_price,
                         fees,
                         ioi_id,
                         order_cancel_reason,
                         data_from_date
                from     ms_prod.smarts_raw_data
                where    {date_condition}
                and      {other_condition}
                order by data_from_date,
                         event_id,
                         event_time;
                """.format(date_condition=date_condition,
                           other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

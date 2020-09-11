from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class OrderFillEventDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize with date field.
        """
        self.date_field = 'created::date'
        self.arguments = kwargs

        return self

    def get_query(self):

        date_condition = data_model_date_range(self)

        if self.arguments.get('other_condition', None):
            other_condition = self.arguments.get('other_condition')

        else:
            other_condition = '1=1'

        query = """
                select   order_fill_event_key,
                         event_id,
                         order_id,
                         account_id,
                         client_order_id,
                         api_session_id,
                         created,
                         order_type,
                         price,
                         quantity,
                         trading_pair,
                         side,
                         average_fill_price,
                         cumulative_quantity,
                         cumulative_gross_notional_for_market_buy,
                         remaining_quantity,
                         remaining_gross_notional_for_market_buy,
                         liquidity_indicator,
                         fee,
                         auction_id
                from     order_fill_event
                where    {date_condition}
                and      {other_condition};
                """.format(date_condition=date_condition, other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

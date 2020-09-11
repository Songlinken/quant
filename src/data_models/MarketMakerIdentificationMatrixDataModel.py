from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class MarketMakerIdentificationMatrixDataModel(DataModel):

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
                select  account_id,
                        data_from_date,
                        symbol,
                        place_cancel_time_diff,
                        bid_ask_ratio_median,
                        bid_ask_ratio_std,
                        is_institutional,
                        fill_buy_orders_count,
                        fill_sell_orders_count,
                        fill_buy_orders_quantity,
                        fill_sell_orders_quantity,
                        total_orders,
                        cancel_orders,
                        maker_counts,
                        taker_counts,
                        market_maker
                from    ms_dev.market_maker_identification_matrix
                where   {date_condition}
                and     {other_condition};
                """.format(date_condition=date_condition, other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

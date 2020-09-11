from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class DailyBestPricesSplitByAccountDataModel(DataModel):

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
                select  account_id,
                        event_id,
                        event_date,
                        event_type,
                        date_time,
                        symbol,
                        side,
                        best_price_1,
                        best_price_2,
                        best_price_3,
                        best_price_4,
                        best_price_5,
                        best_price_6,
                        best_price_7,
                        best_price_8,
                        best_price_9,
                        best_price_10,
                        best_account_volume_1,
                        best_account_volume_2,
                        best_account_volume_3,
                        best_account_volume_4,
                        best_account_volume_5,
                        best_account_volume_6,
                        best_account_volume_7,
                        best_account_volume_8,
                        best_account_volume_9,
                        best_account_volume_10,
                        best_price_1_other_side,
                        best_price_2_other_side,
                        best_price_3_other_side,
                        best_price_4_other_side,
                        best_price_5_other_side,
                        best_price_6_other_side,
                        best_price_7_other_side,
                        best_price_8_other_side,
                        best_price_9_other_side,
                        best_price_10_other_side,
                        best_account_volume_1_other_side,
                        best_account_volume_2_other_side,
                        best_account_volume_3_other_side,
                        best_account_volume_4_other_side,
                        best_account_volume_5_other_side,
                        best_account_volume_6_other_side,
                        best_account_volume_7_other_side,
                        best_account_volume_8_other_side,
                        best_account_volume_9_other_side,
                        best_account_volume_10_other_side
                from    ms_dev.daily_best_prices_split_by_account
                where   {date_condition}
                and     {other_condition};
                """.format(date_condition=date_condition, other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class DailyBestPricesDataModel(DataModel):

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
                        best_volume_1,
                        best_volume_2,
                        best_volume_3
                from    ms_dev.daily_best_prices
                where   {date_condition}
                and     {other_condition};
                """.format(date_condition=date_condition, other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

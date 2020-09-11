from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class LastTradeAndTradeBackDetailDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize with date field.
        """
        self.date_field = 'event_date'
        self.arguments = kwargs

        return self

    def get_query(self):

        if not self.arguments['stored_type']:
            raise ValueError('Need stored_type to run get_query')

        if not self.arguments['release_version']:
            raise ValueError('Need release_version to run get_query')

        date_condition = data_model_date_range(self)

        if self.arguments.get('other_condition', None):
            other_condition = self.arguments.get('other_condition')

        else:
            other_condition = '1=1'

        query = """
                select  stored_type,
                        release_version,
                        account_id,
                        event_id,
                        event_date,
                        event_type,
                        date_time,
                        symbol,
                        side,
                        last_traded_price,
                        price_change_pct,
                        time_from_last_trade,
                        trade_value,
                        counter_party_account_id,
                        trade_back_time,
                        trade_back_price,
                        trade_time_range,
                        money_pass
                from    ms_dev.last_trade_and_trade_back_detail
                where   stored_type = '{stored_type}'
                and     release_version = {release_version}
                and     {date_condition}
                and     {other_condition};
                """.format(stored_type=self.arguments['stored_type'],
                           release_version=self.arguments['release_version'],
                           date_condition=date_condition,
                           other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

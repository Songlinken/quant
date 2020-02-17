from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import execute_query_data_frame


class CryptoPairsDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize with date field.
        """
        self.arguments = kwargs

        return self

    def get_query(self):

        if self.arguments.get('other_condition', None):
            other_condition = self.arguments.get('other_condition')

        else:
            other_condition = '1=1'

        query = """
                select   symbol,
                         to_currency,
                         from_currency,
                         trading_pair,
                         base,
                         quantity_currency,
                         notional,
                         price_currency,
                         fee_currency
                from     public.trading_pairs
                where    {other_condition};
                """.format(other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

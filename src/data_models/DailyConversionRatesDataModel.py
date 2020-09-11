from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class DailyConversionRatesDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize with date field.
        """
        self.date_field = 'created'
        self.arguments = kwargs

        return self

    def get_query(self):

        date_conditions = data_model_date_range(self)

        query = """
                select  created,
                        daily_conversion_rates_id,
                        price,
                        pricing_source,
                        trading_pair
                from    daily_conversion_rates
                where   {date_conditions};
                """.format(date_conditions=date_conditions)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'engine', keyword_arguments=self.arguments)

        return data_frame

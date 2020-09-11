from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class AccountDerivedMetaDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize with date field.
        """
        self.date_field = 'last_modified::date'
        self.arguments = kwargs

        return self

    def get_query(self):

        date_condition = data_model_date_range(self)

        if self.arguments.get('other_condition', None):
            other_condition = self.arguments.get('other_condition')

        else:
            other_condition = '1=1'

        query = """
                set timezone to 'EST';
                select   exchange_account_id,
                         user_or_account_name,
                         name_source,
                         country_code,
                         state_code,
                         is_institutional,
                         first_verified_at at time zone 'EST' as first_verified_at,
                         is_active,
                         last_modified at time zone 'EST' as last_modified,
                         account_group_id
                from     account_derived_metadata
                where    {date_condition}
                and      {other_condition};
                """.format(date_condition=date_condition, other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

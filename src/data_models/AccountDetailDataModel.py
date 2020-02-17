from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import execute_query_data_frame


class AccountDetailDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize with filter field.
        """
        self.arguments = kwargs

        return self

    def get_query(self):

        if self.arguments.get('id'):
            filter_condition = 'id in {filter_field}'.format(filter_field=self.arguments.get('id'))
            filter_condition = filter_condition.replace('[', '(').replace(']', ')')

        else:
            filter_condition = '1=1'

        query = """
                select  id,
                        name,
                        last_modified_at,
                        expecting_wire_rebate,
                        coin_etf_ap,
                        signup_referral_code,
                        account_group_id
                from    exchange_accounts
                where   {filter_condition};
                """.format(filter_condition=filter_condition)

        return query

    def evaluate(self):

        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemini')

        return data_frame

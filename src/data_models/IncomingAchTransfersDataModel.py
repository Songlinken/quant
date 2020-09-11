from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import execute_query_data_frame


class IncomingAchTransfersDataModel(DataModel):

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
                select   id,
                         payment_method_id,
                         cents,
                         is_micro_deposit,
                         requested,
                         current_status,
                         status_last_updated_at,
                         user_id,
                         advance_voucher,
                         exchange_account_id,
                         bank_partner,
                         transport_scheme,
                         ach_currency
                from     incoming_ach_transfers
                where    {other_condition};
                """.format(other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemini', ssh='datalab_prod', keyword_arguments=self.arguments)

        return data_frame

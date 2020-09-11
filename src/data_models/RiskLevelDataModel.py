from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import execute_query_data_frame


class RiskLevelDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize.
        """
        self.arguments = kwargs

        return self

    def get_query(self):

        if self.arguments.get('other_condition', None):
            other_condition = self.arguments.get('other_condition')

        else:
            other_condition = '1=1'

        query = """
                with distinct_account as (
                    select            seu.id as distinct_exchange_user_id,
                                      min(coalesce(exchange_account_id, sea.id)) as exchange_account_id
                    from              public.exchange_users seu
                    left outer join   public.user_account_group_roles uagr
                    on                seu.id = uagr.exchange_user_id
                    left outer join   public.account_group ag
                    on                ag.account_group_id = uagr.account_group_id
                    left outer join   public.user_account_roles uar
                    on                seu.id = uar.exchange_user_id
                    left outer join   public.exchange_accounts sea
                    on                sea.account_group_id = ag.account_group_id
                    group by 1
                )
                select       exchange_account_id,
                             seu.level
                from         public.exchange_users seu
                inner join   distinct_account da
                on           seu.id = da.distinct_exchange_user_id
                where        {other_condition};
                """.format(other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemini', ssh='datalab_prod', keyword_arguments=self.arguments)

        return data_frame

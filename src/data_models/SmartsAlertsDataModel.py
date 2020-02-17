from src.data_models.DataModel import DataModel
from src.utility.DataModelUtility import data_model_date_range, execute_query_data_frame


class SmartsAlertsDataModel(DataModel):

    def initialize(self, **kwargs):
        """
        Initialize with date field.
        """
        self.date_field = 'date'
        self.arguments = kwargs

        return self

    def get_query(self):

        date_condition = data_model_date_range(self)

        if self.arguments.get('other_condition', None):
            other_condition = self.arguments.get('other_condition')

        else:
            other_condition = '1=1'

        query = """
                select  transid,
                        id,
                        setname,
                        market,
                        startdate,
                        starttime,
                        starttimestamp,
                        date,
                        time,
                        timestamp,
                        code,
                        securityid,
                        securitycode,
                        houseid,
                        housecode,
                        clientid,
                        clientcode,
                        intensity,
                        extrafolder,
                        viewer,
                        commandline,
                        shorttext,
                        longtext,
                        usercode,
                        attachmentfilename,
                        attachmentcontent,
                        isreissue
                from    ms_prod.smarts_alerts
                where   {date_condition}
                and     {other_condition};
                """.format(date_condition=date_condition, other_condition=other_condition)

        return query

    def evaluate(self):
        query = self.get_query()
        data_frame = execute_query_data_frame(query, 'gemrdsdb', keyword_arguments=self.arguments, ssh='interim')

        return data_frame

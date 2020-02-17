import datetime
import pandas as pd

from src.data_models.SmartsCsvDataModel import SmartsCsvDataModel
from src.data_models.CryptoPairsDataModel import CryptoPairsDataModel
from src.utility.GeneralUtility import timer
from src.utility.DataModelUtility import data_frame_to_sql


@timer
def write_to_smarts_data(instrument, evaluation_date):
    """
    Pull data from local machine, otherwise download data from datalab and write to database.
    Write data for one day per time.

    :param instrument: str or list (Example: 'BTCUSD' or ['BTCUSD', 'LTCUSD'])
    :param evaluation_date: date
    :param begin_date: date
    :param download_data: bool
    :param unzip: bool
    :param force: bool (override downloaded csv if existing)
    :return: dict with pd dataframe
    """
    smarts_data_dict = SmartsCsvDataModel(download_data=True).evaluate(instrument, evaluation_date, evaluation_date, use_db=False)
    smarts_data = pd.concat(smarts_data_dict).reset_index(drop=True)

    # add additional column as identification in database
    smarts_data['data_from_date'] = pd.to_datetime(evaluation_date)

    data_frame_to_sql(smarts_data, 'smarts_data')


if __name__ == '__main__':
    instruments = CryptoPairsDataModel().evaluate()['trading_pair'].tolist()
    insert_date = datetime.datetime.today().date() - datetime.timedelta(days=1)

    write_to_smarts_data(instruments, insert_date)

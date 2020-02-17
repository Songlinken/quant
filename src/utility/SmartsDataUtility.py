import dask.dataframe as dkd
import os

from src.utility.Configuration import Configuration
from src.utility.GeneralUtility import date_range, random_string, timer


def anonymize_account(data_set, anonymous_str_size=8):
    """
    Attention: This function only works for the alerts excel exported from SMARTS front end.
    """
    # data type correction
    for column in ['Cmd', 'Alert Attachment', 'Comments']:
        data_set[column] = data_set[column].astype(str)

    accounts_info = data_set['Long Text'].str.findall(r'(?<=Broker )\d+').tolist()
    temp_accounts_first = set([account for group in accounts_info for account in group])
    temp_accounts_second = set(data_set['Account ID Code'].dropna().astype(int).astype(str).tolist())
    accounts = list(temp_accounts_first.union(temp_accounts_second))

    anonymized_accounts = []
    for i in range(len(accounts)):
        anonymized_str = random_string(anonymous_str_size)
        anonymized_accounts.append(anonymized_str)

    map_dict = dict(zip(accounts, anonymized_accounts))

    # anonymize account information
    for index_, alert in data_set.iterrows():
        # wash sale A-B-A does not have account information in Account ID Code column
        sub_accounts = set(accounts_info[index_])
        if not sub_accounts:
            sub_accounts = [str(int(alert['Account ID Code']))]

        sub_map_dict = {key: value for key, value in map_dict.items() if key in sub_accounts}

        for column in ['Cmd', 'Alert Attachment', 'Comments', 'Long Text']:
            for account, map_to in sub_map_dict.items():
                alert[column] = alert[column].replace(account, map_to)
            data_set.loc[index_, column] = alert[column]

    data_set['Account ID Code'] = data_set['Account ID Code'].dropna().astype(int).astype(str).map(map_dict)
    data_set['Account ID Name'] = data_set['Account ID Name'].dropna().astype(int).astype(str).map(map_dict)

    return data_set


@timer
def download_smarts_data(instrument, evaluation_date, begin_date, remote_server=None, local_directory='', unzip=True, force=False):
    """
    Copy instrument data from /srv/datalab/smarts folder to local directory.

    :param instrument: str or list (Example: 'BTCUSD' or ['BTCUSD', 'LTCUSD'])
    :param evaluation_date: date
    :param begin_date: date
    :param remote_server: str
    :param local_directory: str
    :param unzip: bool
    :param force: bool (always download for self defined local directory)
    """
    if isinstance(instrument, str):
        instrument = [instrument]

    if remote_server is None:
        username = Configuration.get().get('smarts')[0]['username']
        remote_server = username + '@datalab.devtest.ny2.projecticeland.net'

    date_list = date_range(begin_date, evaluation_date)

    for pair in instrument:

        for dates in date_list:

            year = dates.year
            month = dates.month
            day = dates.day

            if month in range(1, 10):
                # add padding 0
                month = "%02d" % month

            if day in range(1, 10):
                # add padding 0
                day = "%02d" % day

            local_path = ''
            if local_directory == '':
                home = os.path.expanduser('~')
                local_path = home + '/Documents/Smarts_data/{pair}/{year}/{month}/'.format(pair=pair, year=year, month=month)

                if not os.path.exists(local_path):
                    os.makedirs(local_path)

            remote_directory = 'srv/datalab/smarts/{year}/{month}/'.format(year=year, month=month)

            file = '{year}{month}{day}.orders_{pair}.csv'.format(year=year, month=month, day=day, pair=pair)

            if force:
                # download files to local
                os.system('scp -r ' + remote_server + ':/' + remote_directory + file + '.gz ' + str({local_path, local_directory}).replace(' ', '').replace("'", ''))

                if unzip:

                    if local_path != '':
                        os.system('gunzip -f ' + local_path + file + '.gz')

                    else:
                        os.system('gunzip -f ' + local_directory + file + '.gz')

            else:
                # download files only if not exist
                if not os.path.exists(local_path + file):
                    os.system('scp -r ' + remote_server + ':/' + remote_directory + file + '.gz ' + str({local_path, local_directory}).replace(' ', '').replace("'", ''))

                    if unzip:

                        if local_path != '':
                            os.system('gunzip -f ' + local_path + file + '.gz')

                        else:
                            os.system('gunzip -f ' + local_directory + file + '.gz')


@timer
def read_csv_to_dk_dataframe(instrument, evaluation_date, begin_date, local_directory=None):
    """
    Read csv files to dask dataframe.

    :param instrument: str or list (Example: 'BTCUSD' or ['BTCUSD', 'LTCUSD'])
    :param evaluation_date: date
    :param begin_date: date
    :param local_directory: str
    :return: dict with dask dataframe
    """
    if isinstance(instrument, str):
        instrument = [instrument]

    if local_directory is None:
        # read csv from smartsdata folder by default
        home = os.path.expanduser('~')
        local_directory = home + '/Documents/Smarts_data/'

    date_list = date_range(begin_date, evaluation_date)

    dk_final_data_dict = {}
    for pair in instrument:
        trade_in = pair[:3]
        trade_to = pair[-3:]

        file_list = []
        for date in date_list:

            year = date.year
            month = date.month
            day = date.day

            if month in range(1, 10):
                # add padding 0
                month = "%02d" % month

            if day in range(1, 10):
                # add padding 0
                day = "%02d" % day

            file_list.append(local_directory + '{pair}/{year}/{month}/{year}{month}{day}.orders_{instrument}.csv'
                             .format(pair=pair, year=year, month=month, day=day, instrument=pair))

        dk_data_set = []
        for file in file_list:
            # dask dataframe works better when data is large
            data = dkd.read_csv(file, dtype={'Event ID': 'int64',
                                             'Event Date': 'object',
                                             'Event Time': 'object',
                                             'Event Millis': 'object',
                                             'Order ID': 'int64',
                                             'Execution Options': 'object',
                                             'Event Type': 'object',
                                             'Symbol': 'object',
                                             'Order Type': 'object',
                                             'Side': 'object',
                                             'Limit Price ({trade_to})'.format(trade_to=trade_to): 'float64',
                                             'Original Quantity ({trade_in})'.format(trade_in=trade_in): 'float64',
                                             'Gross Notional Value ({trade_to})'.format(trade_to=trade_to): 'float64',
                                             'Fill Price ({trade_to})'.format(trade_to=trade_to): 'float64',
                                             'Fill Quantity ({trade_in})'.format(trade_in=trade_in): 'float64',
                                             'Total Exec Quantity ({trade_in})'.format(trade_in=trade_in): 'float64',
                                             'Remaining Quantity ({trade_in})'.format(trade_in=trade_in): 'float64',
                                             'Avg Price ({trade_to})'.format(trade_to=trade_to): 'float64',
                                             'Fees ({trade_to})'.format(trade_to=trade_to): 'float64',
                                             'Auction ID': 'float64',
                                             'Account ID': 'int64',
                                             'IOI ID': 'float64',
                                             'Order Cancel Reason': 'object'})
            dk_data_set.append(data)

        dk_data_set = dkd.concat(dk_data_set)

        dk_final_data_dict.update({pair: dk_data_set})

    return dk_final_data_dict

from alpha_vantage.cryptocurrencies import CryptoCurrencies

from utility.Configuration import Configuration
from utility.GeneralUtility import timer


@timer
def get_daily_crypto_data(symbol, market, output_format='pandas'):
    """
    Get daily crypto data from alpha_vantage and return pandas dataframe.

    :param key: string
    :param symbol: string or list
    :param market: string
    :param output_format: string
    :return: dict
    """
    config = Configuration.get().get('alpha_vantage')[0]['key']
    if not config:
        raise Exception('Alpha Vantage configuration is not set properly.')

    if isinstance(symbol, str):
        symbol = [symbol]

    crypto_currency_object = CryptoCurrencies(key=config, output_format=output_format)

    data_dict = {}
    for instrument in symbol:
        data, meta_data = crypto_currency_object.get_digital_currency_daily(symbol=instrument, market=market)
        data = data.reset_index()
        data['instrument'] = instrument
        data['timezone'] = meta_data['7. Time Zone']
        data['date'] = data['date'].astype('datetime64')

        returned_columns = {
            'instrument': 'Instrument',
            'date': 'Price_date',
            '1a. open (USD)': 'Open_price',
            '2a. high (USD)': 'High_price',
            '3a. low (USD)': 'Low_price',
            '4a. close (USD)': 'Close_price',
            '6. market cap (USD)': 'Market_price_cap',
            '5. volume': 'Volume',
            'timezone': 'Timezone'
        }

        data = data.rename(columns=returned_columns)[returned_columns.values()]

        data_dict.update({instrument: data})

    return data_dict

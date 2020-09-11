import numpy as np
import plotly.graph_objs as go
import plotly.offline as offplt
import talib as ta

from scipy.fftpack import fft

from src.QuantML.data_source.CryptoCompare import CryptoCompare

offplt.offline.init_notebook_mode(connected=True)


def candlesticks_with_mas(data_set, instrument, fastperiod=12, slowperiod=26, signalperiod=9, rsi_short=6, rsi_mid=12, rsi_long=24, plot_out=False):

    # calculate moving averages indicators.
    data_set['ma_5'] = data_set['close'].rolling(window=5).mean()
    data_set['ma_10'] = data_set['close'].rolling(window=10).mean()
    data_set['ma_30'] = data_set['close'].rolling(window=30).mean()
    data_set['ma_60'] = data_set['close'].rolling(window=60).mean()
    data_set['ma_120'] = data_set['close'].rolling(window=120).mean()
    data_set['ma_240'] = data_set['close'].rolling(window=240).mean()

    # calculate macd indicator: dif = ema12 - ema26; dem = ema9 of dif; osc = dif - dem
    data_set['dif'], data_set['dem'], data_set['osc'] = ta.MACD(data_set['close'], fastperiod, slowperiod, signalperiod)

    # calculate rsi
    data_set['rsi_6'] = ta.RSI(data_set['close'], rsi_short)
    data_set['rsi_12'] = ta.RSI(data_set['close'], rsi_mid)
    data_set['rsi_24'] = ta.RSI(data_set['close'], rsi_long)

    # prepare color indicator.
    data_set.loc[data_set['close'].diff() > 0, 'color'] = 'green'
    data_set['color'] = data_set['color'].fillna('red')

    # prepare candlesticks data.
    candlesticks_data = go.Candlestick(x=data_set['time'],
                                       open=data_set['open'],
                                       high=data_set['high'],
                                       low=data_set['low'],
                                       close=data_set['close'],
                                       increasing=dict(line=dict(color='green')),
                                       decreasing=dict(line=dict(color='red')),
                                       name='Price')

    # prepare trading volume data.
    trading_volume_data = go.Bar(x=data_set['time'],
                                 y=data_set['volumefrom'],
                                 yaxis='y2',
                                 marker=dict(color=data_set['color']),
                                 name='Volume')

    # prepare moving average data.
    mv_5_data = go.Scattergl(x=data_set['time'], y=data_set['ma_5'], mode='lines', name='MV-5')
    mv_10_data = go.Scattergl(x=data_set['time'], y=data_set['ma_10'], mode='lines', name='MV-10')
    mv_30_data = go.Scattergl(x=data_set['time'], y=data_set['ma_30'], mode='lines', name='MV-30')
    mv_60_data = go.Scattergl(x=data_set['time'], y=data_set['ma_60'], mode='lines', name='MV-60')
    mv_120_data = go.Scattergl(x=data_set['time'], y=data_set['ma_120'], mode='lines', name='MV-120')
    mv_240_data = go.Scattergl(x=data_set['time'], y=data_set['ma_240'], mode='lines', name='MV-240')

    # prepare macd data.
    dif_data = go.Scattergl(x=data_set['time'], y=data_set['dif'], yaxis='y3', mode='lines', name='DIF')
    dem_data = go.Scattergl(x=data_set['time'], y=data_set['dem'], yaxis='y3', mode='lines', name='DEM')
    osc_data = go.Scatter(x=data_set['time'], y=data_set['osc'], fill='tozeroy', yaxis='y3', mode='lines', name='OSC')

    # prepare rsi data.
    rsi_6_data = go.Scattergl(x=data_set['time'], y=data_set['rsi_6'], yaxis='y4', mode='lines', name='RSI-6')
    rsi_12_data = go.Scattergl(x=data_set['time'], y=data_set['rsi_12'], yaxis='y4', mode='lines', name='RSI-12')
    rsi_24_data = go.Scattergl(x=data_set['time'], y=data_set['rsi_24'], yaxis='y4', mode='lines', name='RSI-24')

    # prepare layout.
    layout = go.Layout(
        title='BTC Historical Price',
        xaxis=dict(
            title='Date',
            showgrid=True,
            rangeslider=dict(
                visible=False
            )
        ),
        yaxis=dict(title='Price', domain=[0.5, 1], tickformat='$,'),
        yaxis2=dict(domain=[0, 0.1], anchor='y2', tickformat=','),
        yaxis3=dict(showticklabels=False, domain=[0.1, 0.3], anchor='y3'),
        yaxis4=dict(domain=[0.3, 0.4], anchor='y4')
    )

    final_data = [candlesticks_data, trading_volume_data, mv_5_data, mv_10_data, mv_30_data, mv_60_data, mv_120_data,
                  mv_240_data, dif_data, dem_data, osc_data, rsi_6_data, rsi_12_data, rsi_24_data]

    fig = dict(data=final_data, layout=layout)

    if plot_out:
        offplt.plot(fig, filename='candlesticks_with_mas_{}.html'.format(instrument))

    return fig


def prediction_with_fft(data_set, instrument, plot_out=False):

    data_set['price_movement'] = data_set['close'].diff()
    data_set['price_movement_adjusted'] = data_set['price_movement'].ewm(span=30).mean()

    x_frequency = np.arange(int(len(data_set) / 2))
    y_fft = abs(fft(data_set['price_movement'][1:])[x_frequency])

    # plot FFT result
    fft_result = go.Scattergl(x=x_frequency, y=y_fft, mode='lines')

    layout = go.Layout(
        title='{}: FFT of Price Movement'.format(instrument),
        xaxis=dict(title='Frequency', showgrid=True),
        yaxis=dict(title='Amplitude', tickformat=',')
    )

    data = [fft_result]
    fig = dict(data=data, layout=layout)

    if plot_out:
        offplt.plot(fig, filename='fft_of_price_movement_{}.html'.format(instrument))

    return fig


if __name__ == '__main__':
    data = CryptoCompare().get_historical_data_to_dataframe('BTC', 'USD', True)
    # fft_plot = prediction_with_fft(data, 'BTCUSD', plot_out=True)
    plot = candlesticks_with_mas(data, 'BTC', plot_out=True)

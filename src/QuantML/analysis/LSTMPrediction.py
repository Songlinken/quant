import datetime
import numpy as np
import pywt

from keras.models import Model
from keras.layers import Dense, Input
from sklearn.preprocessing import MinMaxScaler

from src.QuantML.data_models.CCCAGGHistoryDataModel import CCCAGGHistoryDataModel
from src.QuantML.data_models.GeminiHistoryDataModel import GeminiHistoryDataModel
from src.QuantML.features.Features import commodity_channel_index


class LSTMPrediction(object):

    def get_data(self, exchange, start_date, evaluation_date, instrument):

        if exchange == 'gemini':
            data_model = GeminiHistoryDataModel()

        elif exchange == 'cccagg':
            data_model = CCCAGGHistoryDataModel()

        else:
            raise ValueError('exchange mush be either "gemini" or "cccagg".')

        data_set = (data_model
                    .initialize(start_date=start_date,
                                evaluation_date=evaluation_date,
                                other_condition="symbol = '{}'".format(instrument))
                    .evaluate())

        return data_set

    def wavelet_transformation(self, data_set):
        approximation, noise = pywt.dwt(data_set['close_price'], 'haar')

        approximation_soft = pywt.threshold(approximation, np.std(approximation), mode='soft')
        noise_soft = pywt.threshold(noise, np.std(noise), mode='soft')

        data_set['wavelet_trans'] = pywt.idwt(approximation_soft, noise_soft, 'haar')

        return data_set

    def stack_auto_encode(self, data_set):
        # encode
        normalized_wavelet_trans_diff_list = []
        data_set['wavelet_trans'].diff().rolling(10).apply(lambda x: normalized_wavelet_trans_diff_list.append(x.tolist()) or 0)
        normalized_wavelet_trans_diff_list = [MinMaxScaler().fit_transform(np.reshape(x, (-1, 1))) for x in normalized_wavelet_trans_diff_list]
        normalized_wavelet_trans_diff_array = np.asanyarray([x.T for x in normalized_wavelet_trans_diff_list])
        import ipdb;ipdb.set_trace()
        input_data = Input(shape=(1, 10))
        encode_1 = Dense(5, activation='relu')(input_data)
        encode_2 = Dense(2, activation='relu')(encode_1)

        # decode
        decode_1 = Dense(5, activation='relu')(encode_2)
        decode_2 = Dense(10, activation='sigmoid')(decode_1)

        autoencoder = Model(input=input_data, output=decode_2)
        encoder = Model(input=input_data, output=encode_2)

        # compile
        autoencoder.compile(optimizer='adam', loss='mse')

        autoencoder.fit(normalized_wavelet_trans_diff_array, normalized_wavelet_trans_diff_array, epochs=20)
        import ipdb;ipdb.set_trace()

if __name__ == '__main__':
    model = LSTMPrediction()
    data = model.get_data('cccagg', datetime.date(2019, 1, 1), datetime.date(2019, 10, 31), 'BTCUSD')
    data_noise_removel = model.wavelet_transformation(data)
    data_with_encode = model.stack_auto_encode(data_noise_removel)

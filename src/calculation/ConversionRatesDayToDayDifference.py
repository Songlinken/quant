import datetime

from src.calculation.CalculationNode import CalculationNode
from src.data_models.DailyConversionRatesDataModel import DailyConversionRatesDataModel


class ConversionRatesDayToDayDifference(CalculationNode):

    def get_data(self):

        data_set = DailyConversionRatesDataModel().initialize(
            start_date=datetime.date(2018, 1, 1)
        ).evaluate()

        return data_set

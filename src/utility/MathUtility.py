import numpy as np


def sigmoid(array, numerator=1.0, scale_factor=1, intercept=0):
    """
    :param array: np array
    :param numerator: numeric
    :param scale_factor: numeric
    :param intercept: numeric
    :return: np array
    """
    return numerator / (1 + np.exp(-scale_factor * array)) + intercept

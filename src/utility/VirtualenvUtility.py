import os
import sys


def have_virtualenv():
    """
    Check existence of virtualenv.
    """
    bin_dir = os.path.dirname(sys.executable)

    return 'activate' in os.listdir(bin_dir)


def get_virtualenv_root():
    """
    Check the environment which code is running in.
    If not in a virtualenv but there is a virtualenv existing, get the path.
    """
    virtualenv = os.environ.get('VIRTUAL_ENV')

    if not virtualenv and have_virtualenv():

        bin_dir = os.path.dirname(sys.executable)
        virtualenv = os.path.dirname(bin_dir)

    return virtualenv

# -*- coding: utf-8 -*-

__author__ = 'Benoit Barthelet'
__email__ = 'benoit.barthelet@gmail.com'
__version__ = '0.1.0'


# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())
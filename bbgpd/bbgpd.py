# -*- coding: utf-8 -*-
import logging

from blpapi import Session, Event
from blpapi.request import Request
import pandas as pd

log = logging.getLogger(__name__)


class BLP(object):
    def __init__(self):
        """
        Create a synchronous blp object by starting a session with default settings
        """
        self.session = Session()
        self.session.start()
        self.session.openService('//BLP/refdata')
        self.refDataSvc = self.session.getService('//BLP/refdata')

    def closeSession(self):
        """
        Close the session
        """
        self.session.stop()

    def bdp(self, ticker_l: list, field_l: list, override_l: list = None, debug: bool = False) -> pd.DataFrame:
        """
        Bloomberg BDP implementation
        :param ticker_l: list of tickers
        :param field_l: list of fields
        :param override_l: list of dictionnaries {"override key": "override value"}
        :param debug: boolean
        :return: returns a DataFrame with the security as the column, the fields as the rows
        """

        # set debub level
        if debug:
            log.setLevel(logging.DEBUG)
        else:
            log.setLevel(logging.INFO)

        request_type = self.refDataSvc.createRequest('ReferenceDataRequest')

        request = self._build_request(request_type, ticker_l, field_l, override_l)
        self.session.sendRequest(request)

        r = {}
        while 1:
            event = self.session.nextEvent()
            log.debug('event type: {}'.format(event.eventType()))
            for msg in event:
                log.debug(msg)
                if msg.hasElement('responseError'):
                    log.error(msg)
                    break
                elif msg.hasElement(("securityData")):
                    sec_data_arr = msg.getElement("securityData")
                    log.debug(sec_data_arr)
                    for i in range(sec_data_arr.numValues()):
                        sec_data = sec_data_arr.getValueAsElement(i)
                        ticker = sec_data.getElement('security').getValueAsString()
                        if sec_data.hasElement('securityError'):
                            log.error("wrong ticker")
                        else:
                            fieldData = sec_data.getElement('fieldData')
                            f = {}
                            for name in field_l:
                                if fieldData.hasElement(name):
                                    field = fieldData.getElement(name)
                                    # case for FUT_CHAIN-type response
                                    if field.isArray():
                                        f[name] = []
                                        for j in range(field.numValues()):
                                            for k in range(field.getValueAsElement(j).numElements()):
                                                # f[name].append(field.getValueAsElement(j).
                                                # getElement('Security Description').getValue())
                                                # changed the 'Security Description' to 0, could be also
                                                # 'Member Ticker and Exchange Code' for
                                                # MEMB type responses
                                                f[name].append(field.getValueAsElement(j).getElement(k).getValue())
                                    # general case
                                    else:
                                        f[name] = field.getValue()
                                else:
                                    pass
                            r[ticker] = f
            if event.eventType() == Event.RESPONSE:
                break
        return pd.DataFrame(r)

    def _build_request(self, request_type, ticker_l: list, field_l: list, override_l: list) -> Request:
        """
        Generic  private method to build a request
        Enforce that tickers, fields are lists and that overrides are list of dictionnaries with
        {"override key": "override value"}
        :param request_type: blpapi.request.Request, should be ReferenceDataRequest for bdp and HistoricalDataRequest
        for bdh
        :param ticker_l: list of tickers
        :param field_l: list of fields
        :param override_l: list of dictionnaries {"override key": "override value"}
        :return: a blpapi.request.Request object with the passed tickers, fields and overrides
        """

        for arg in [ticker_l, field_l, override_l]:
            if arg:
                if type(arg) is not list:
                    log.error("{} must be a list".format(arg))
                    raise TypeError("Argument {} must be a list".format(arg))
                if arg is override_l:
                    for a in arg:
                        if type(a) is not dict:
                            log.error("{} must be a list of dict, element {} is a {}".format(arg, a, type(a)))
                            raise TypeError("{} must be a list of dict, element {} is a {}".format(arg, a, type(a)))

        for ticker in ticker_l:
            request_type.append('securities', ticker)
            log.debug('ticker added to blpapi request object in securities: {}'.format(ticker))
        for field in field_l:
            request_type.append('fields', field)
            log.debug('field added to blpapi request object in fields: {}'.format(field))
        if override_l:
            request_overrides = request_type.getElement('overrides')
            for override in override_l:
                request_override = request_overrides.appendElement()
                for key, val in override.items():
                    request_override.setElement('fieldId', key)
                    request_override.setElement('value', val)

        return request_type
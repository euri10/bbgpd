#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_bbgpd
----------------------------------

Tests for `bbgpd` module.
"""
import logging

import blpapi
import pytest


@pytest.fixture
def blp():
    from bbgpd.bbgpd import BLP
    return BLP()


@pytest.fixture(params=[True, False])
def debugflag(request):
    logging.basicConfig()
    return request.param


def test_connection(blp):
    assert isinstance(blp.session, blpapi.Session)
    blp.closeSession()


def test_bdp_with_no_ticker_list(blp):
    with pytest.raises(TypeError):
        blp.bdp('VOD LN Equity', ['PX_LAST'], debug=debugflag)


def test_bdp_with_no_field_list(blp):
    with pytest.raises(TypeError):
        blp.bdp(['VOD LN Equity'], 'PX_LAST', debug=debugflag)


def test_bdp_with_no_override_list(blp):
    with pytest.raises(TypeError):
        blp.bdp(['VOD LN Equity'], ['PX_LAST'], override_l={'EQY_FUND_CRNCY': 'JPY'}, debug=debugflag)


def test_bdp_with_override_list_but_no_dict_inside(blp):
    with pytest.raises(TypeError):
        blp.bdp(['VOD LN Equity'], ['PX_LAST'], override_l=[('EQY_FUND_CRNCY', 'JPY')], debug=debugflag)

def test_bdp_chain_type_returns_list(blp):
    optovd = [{'CHAIN_STRIKE_PX_OVRD': '80%-120%'}, {'CHAIN_EXP_DT_OVRD': '1F-5F'}, {'CHAIN_PUT_CALL_TYPE_OVRD': 'P'}]
    ret = blp.bdp(['SX5E Index'], ['CHAIN_TICKERS'], override_l=optovd)
    assert isinstance(ret['SX5E Index']['CHAIN_TICKERS'], list)
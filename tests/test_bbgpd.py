#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_bbgpd
----------------------------------

Tests for `bbgpd` module.
"""
import logging
import datetime
import blpapi
import pytest
import pandas as pd


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


def test_bdp_returns_df(blp):
    dfret = blp.bdp(['EUR Curncy'], ['Name'])
    df = pd.DataFrame(data={'EUR Curncy': 'Euro Spot'}, index=pd.Index(['Name']))
    assert dfret.equals(df) is True


def test_bdh_verysimple(blp):
    pnret = blp.bdh(['FP FP Equity'], ['PX_LAST'], datetime.date(2016, 7, 8), datetime.date(2016, 7, 11),
                    debug=debugflag)

    assert isinstance(pnret, pd.Panel)
    response = {'FP FP Equity': pd.DataFrame(index=pd.DatetimeIndex([datetime.date(2016, 7, 8), datetime.date(2016, 7, 11)]),
                                             columns=['PX_LAST'], data=[42.65, 43.35])}

    pn = pd.Panel.from_dict(response)
    print(pn)
    print(pnret)

    print(pn.items)
    print(pnret.items)

    print(pn.major_axis)
    print(pnret.major_axis)

    print(pn.minor_axis)
    print(pnret.minor_axis)

    for i in pn.items:
        print(type(i))
    for i in pnret.items:
        print(type(i))

    for i in pn.major_axis:
        print(type(i))
    for i in pnret.major_axis:
        print(type(i))

    for i in pn.minor_axis:
        print(type(i))
    for i in pnret.minor_axis:
        print(type(i))



    assert pnret.equals(pn) is True

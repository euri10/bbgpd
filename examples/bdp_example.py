import logging

from bbgpd.bbgpd import BLP

logging.basicConfig()
blp = BLP()
price = blp.bdp(['VOD LN Equity', 'IBM US Equity'], ['PX_LAST', ], override_l=[{'CRNCY': 'PY'}], debug=True)
print(price)

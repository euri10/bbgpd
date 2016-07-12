import logging
import datetime
from bbgpd.bbgpd import BLP

logging.basicConfig()
blp = BLP()
# price = blp.bdp(['VOD LN Equity', 'IBM US Equity'], ['PX_LAST', ], override_l=[{'CRNCY': 'PY'}], debug=False)
# print(price)

b = blp.bdh(['FP FP Equity'], ['PX_LAST'], datetime.date(2016, 7, 8), datetime.date(2016, 7, 11), debug=True)
print(b)

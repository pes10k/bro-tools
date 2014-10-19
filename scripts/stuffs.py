#/usr/bin/env python
"""Finds all instances of page requests that fit the following conditions:
    * Request to amazon.com
    * That is a leaf node (ie is not the referrer to any requests)
    * includes the affiliate cookie tag (&tag= or ?tag=)
    * is a request collected by the `extract.py` script (so requests for
      html documents only)
"""

import sys
import os.path
sys.path.append(os.path.join('..'))

import brotools.reports
from stuffing.amazon import is_cookie_set

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

for path, data in ins():
    debug("Considering {0}".format(path))
    debug(" * {0} graphs found".format(len(data)))
    for g in data:
        for n in g.leaves():
            if is_cookie_set(n):
                debug(" * * Found possible url: {0}".format(n.url()))
                chain = g.chain_from_node(n)
                out.write(str(chain))
                out.write("\n-------\n\n")

"""Checks a colleciton of bro records to see what percentage of records that
have referrers we're able to find referrers for."""

import sys
import brotools.reports
import brotools.records

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

debug("Getting ready to start reading {0} graphs".format(count))
index = 0
num_records = 0
num_rec_with_referrer = 0
for path, graphs in ins():
    index += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    debug("{0}-{1}. Found {2} graphs".format(index, count, len(graphs)))
    for g in graphs:
        num_records += len(g)
        if g._root.referrer:
            num_rec_with_referrer += len(g)
        else:
            num_rec_with_referrer += len(g) - 1

out.write("# records found: {0}\n".format(num_records))
out.write("# w/ referrer:   {0}\n".format(num_rec_with_referrer))
out.write("Radio: {0}\n".format(float(num_rec_with_referrer) / float(num_records)))

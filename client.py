"""Extracts browsing information about a client during a given period of
time.  Input data is read from collections of pickled BroRecordGraphs"""

import sys
import brotools.reports
import brotools.records
import dateutil.parser
import time

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--ip', default=None,
                    help="The IP of the client to track.")
parser.add_argument('--agent', default=None,
                    help="The user agent of the client to track in the bro " +
                    "record data.")
parser.add_argument('--start', default=None,
                    help="A filter for the earliest possible record to " +
                    "consider, as a date string.")
parser.add_argument('--end', default=None,
                    help="A filter for the latest possible record to " +
                    "consider, as a date string.")
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

start_ts = None
if args.start:
    start_ts = time.mktime(dateutil.parser.parse(args.start).timetuple())

end_ts = None
if args.end:
    end_ts = time.mktime(dateutil.parser.parse(args.end).timetuple())

debug("Getting ready to start reading {0} graphs".format(count))
index = 0
for path, graphs in ins:
    index += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    debug("{0}-{1}. Found {2} graphs".format(index, count, len(graphs)))
    for g in graphs:

        if args.ip and args.ip != g.ip:
            continue

        if args.agent and args.agent != g.user_agent:
            continue

        if start_ts and g.latest_ts < start_ts:
            break

        if end_ts and g.earliest_ts > end_ts:
            break

        debug(" * Found matching graph with root {0}".format(g._root.url))
        out.write(str(g))
        out.write("\n\n")

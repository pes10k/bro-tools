"""Commandline utility for examining pickled collections of BroRecordGraph
objects, looking for instances of checkouts from Amazon and examining what
requests could have influenced the affiliate marketing cookie that got credit
for the purchase."""

import sys
import brotools.reports
import brotools.records
import stuffing.amazon

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--ttl', type=int, default=84600,
                    help="The time, in seconds, that an Amazon set affiliate " +
                    "marketing cookie is expected to be valid.  Default is " +
                    "one day (84600 seconds)")
parser.add_argument('--secs', type=int, default=3600,
                    help="The minimum time in seconds that must pass between " +
                    "a client's requests to the Amazon 'add to cart' page " +
                    "for those requests to be treated as a seperate checkout")
ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

history_by_client = {}
count = 0
debug("Preparing to start reading pickled data")
for path, graphs in ins:
    count += 1
    debug("{0}. Considering {1}".format(count, path))
    debug("{0}. Found {1} graphs".format(count, len(graphs)))
    for g in graphs:
        hash_key = g.ip + " " + g.user_agent
        try:
            history = history_by_client[hash_key]
            stuffs, sets, carts = history.consider(g)
        except KeyError:
            history = stuffing.amazon.AmazonHistory(g)
            history_by_client[hash_key] = history
            stuffs, sets, carts = history.counts()
        values = stuffs + sets + carts
        if values:
            debug(" * For IP: {0}".format(hash_key))
            debug("-----")
        if stuffs:
            debug(" * Stuffs: {0}".format(stuffs))
        if sets:
            debug(" * Sets  : {0}".format(sets))
        if carts:
            debug(" * Carts : {0}".format(carts))
        if values:
            debug("")

for h in history_by_client.values():
    for c in h.checkouts(seconds=args.secs, cookie_ttl=args.ttl):
        out.write(str(c))
        out.write("\n\n\n")

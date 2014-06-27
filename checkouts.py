"""Commandline utility for examining pickled collections of BroRecordGraph
objects, looking for instances of checkouts from Amazon and examining what
requests could have influenced the affiliate marketing cookie that got credit
for the purchase."""

import sys
import brotools.reports
import brotools.records
from stuffing.godaddy import GodaddyAffiliateHistory
from stuffing.amazon import AmazonAffiliateHistory

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--ttl', type=int, default=84600,
                    help="The time, in seconds, that an Amazon set affiliate " +
                    "marketing cookie is expected to be valid.  Default is " +
                    "one day (84600 seconds)")
parser.add_argument('--secs', type=int, default=3600,
                    help="The minimum time in seconds that must pass between " +
                    "a client's requests to the Amazon 'add to cart' page " +
                    "for those requests to be treated as a seperate checkout")
parser.add_argument('--amazon', action="store_true",
                    help="Whether to look for Amazon cookie stuffing.  Note " +
                    "that if no marketer is specified, all will be used " +
                    "(Amazon, GoDaddy, etc.)")
parser.add_argument('--godaddy', action="store_true",
                    help="Whether to look for GoDaddy cookie stuffing.  Note " +
                    "that if no marketer is specified, all will be used " +
                    "(Amazon, GoDaddy, etc.)")
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

marketers = []
if not args.amazon and not args.godaddy:
    marketers.append(AmazonAffiliateHistory)
    marketers.append(GodaddyAffiliateHistory)

if args.amazon:
    marketers.append(AmazonAffiliateHistory)

if args.godaddy:
    marketers.append(GodaddyAffiliateHistory)

# Multi indexed dict, in the following format:
#
# "Marketer name 1": {
#    "client 1 hash": history object,
#    "client 2 hash": history object
# },
# "Marketer name 2": {
#    "client_hash": history object
# },
history_by_client = {}
index = 0
debug("Preparing to start reading {0} pickled data".format(count))
for path, graphs in ins:
    index += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    debug("{0}-{1}. Found {2} graphs".format(index, count, len(graphs)))
    for g in graphs:
        hash_key = g.ip + " " + g.user_agent
        for marketer in marketers:
            # First extract the dict for this marketer
            try:
                client_dict = history_by_client[marketer.name()]
            except:
                client_dict = {}
                history_by_client[marketer.name()] = client_dict

            # Next, try to extract a history object for this client
            # out of the dict of clients for the given marketer
            try:
                history = client_dict[hash_key]
                stuffs, sets, carts = history.consider(g)
            except KeyError:
                history = marketer(g)
                client_dict[hash_key] = history
                stuffs, sets, carts = history.counts()

            values = stuffs + sets + carts
            if values:
                debug("Marketer: {0}".format(marketer.name()))
                debug("For IP: {0}".format(hash_key))
                debug("-----")
            if stuffs:
                debug(" * Stuffs: {0}".format(stuffs))
            if sets:
                debug(" * Sets  : {0}".format(sets))
            if carts:
                debug(" * Carts : {0}".format(carts))
            if values:
                debug("")
                break

for marketer_name, histories in history_by_client.items():
    for client_hash, h in histories.items():
        for c in h.checkouts(seconds=args.secs, cookie_ttl=args.ttl):
            if len(c.cookie_history()) > 0:
                out.write(str(c))
                out.write("\n\n\n")

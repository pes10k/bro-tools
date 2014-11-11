#!/usr/bin/env python
"""Checks a collection of bro log graphs to measure two values, 1) how many
graph heads have referrer heads, and 2) what hosts each of those referrer heads
are for."""

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import brotools.reports
import brotools.records
import urlparse
import user_agents
from stuffing.amazon import AmazonAffiliateHistory


parser = brotools.reports.marketing_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--ttl', type=int, default=84600,
                    help="The time, in seconds, that an Amazon set " +
                    "affiliate marketing cookie is expected to be valid. " +
                    "Default is one day (84600 seconds)")
parser.add_argument('--secs', type=int, default=3600,
                    help="The minimum time in seconds that must pass " +
                    "between a client's requests to the marketers 'add to " +
                    "cart' page for those requests to be treated as a " +
                    "seperate checkout")
count, ins, out, debug, marketers, args = brotools.reports.parse_marketing_cli_args(parser)

debug("Preparing to reading {0} sets of graphs".format(count))

browser_attributes = (
    'is_mobile',
    'is_tablet',
    'is_touch_capable',
    'is_pc',
    'is_tablet'
)
amazon_ish_hosts = (
    'amazon.com',
    'www.amazon.com',
    'www.amazon.co.uk',
)
num_requests = 0
num_with_no_referrers = 0
num_unmatched_referrers = 0
num_graphs = 0
num_invalid_urls = 0

num_non_browser = 0

num_amazon_requests = 0
num_amazon_roots = 0
num_amazon_no_referrer = 0

amazon_stuff_graphs = []
amazon_checkout_graphs = []
amazon_set_graphs = []
amazon_missing_sess_id = 0

hosts = {}
invalid_referrers = []
index = 0

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

for path, graph in ins():
    index += 1
    num_graphs += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    num_requests += len(graph)

    head_record = graph._root

    parsed_agent = user_agents.parse(head_record.user_agent)

    is_not_browser = not any([getattr(parsed_agent, attr) for attr in browser_attributes])
    if is_not_browser:
        num_non_browser += 1
        continue

    is_amazon_head = "amazon.com" in head_record.host

    if is_amazon_head:
        num_amazon_roots += 1

    if not head_record.referrer:
        num_with_no_referrers += 1
        if is_amazon_head:
            num_amazon_no_referrer += 1
        continue

    num_unmatched_referrers += 1
    try:
        url_parts = urlparse.urlparse("http://{0}".format(head_record.referrer))
    except ValueError:
        num_invalid_urls += 1
        invalid_referrers.append(head_record.referrer)
        continue

    referrer_host = url_parts.netloc
    try:
        hosts[referrer_host] += 1
    except KeyError:
        hosts[referrer_host] = 1

    num_amazon_requests += sum([len(graph.nodes_for_host(h)) for h in amazon_ish_hosts])

    local_amazon_stuff_records = AmazonAffiliateHistory.stuffs_in_graph(graph)
    local_amazon_checkout_records = AmazonAffiliateHistory.checkouts_in_graph(graph)
    local_amazon_set_records = AmazonAffiliateHistory.cookie_sets_in_graph(graph)

    amazon_stuff_graphs += local_amazon_stuff_records
    amazon_checkout_graphs += local_amazon_checkout_records
    amazon_set_graphs += local_amazon_set_records

    # Since we already have to check to see if there are any match graphs,
    # we can save some double duty, and only try and slot new graphs into
    # a purchase history if there is at least one already found interesting
    # graph
    num_local_interesting_graphs = len(local_amazon_stuff_records + local_amazon_checkout_records + local_amazon_set_records)
    if num_local_interesting_graphs == 0:
        continue

    # First extract the dict for this AmazonAffiliateHistory
    try:
        client_dict = history_by_client[AmazonAffiliateHistory.name()]
    except KeyError:
        client_dict = {}
        history_by_client[AmazonAffiliateHistory.name()] = client_dict

    # See if we can find a session tracking cookie for this visitor
    # in this graph.  If not, then we know there are no cookie stuffs,
    # checkouts, or other relevant activity in the graph we
    # care about, so we can continue
    hash_key = AmazonAffiliateHistory.session_id_for_graph(graph)
    if not hash_key:
        amazon_missing_sess_id += 1
        continue

    # Next, try to extract a history object for this client
    # out of the dict of clients for the given AmazonAffiliateHistory
    try:
        client_dict[hash_key].consider(graph)
    except KeyError:
        client_dict[hash_key] = AmazonAffiliateHistory(graph)

out.write("General Stats\n")
out.write("===\n")
out.write("# requests:              {0:10}\n".format(num_requests))
out.write("# graphs:                {0:10}\n".format(num_graphs))
out.write("# browser:               {0:10}\n".format(num_graphs - num_non_browser))
out.write("# no referrer:           {0:10}\n".format(num_with_no_referrers))
out.write("# unmatched referrer:    {0:10}\n".format(num_unmatched_referrers))
out.write("# invalid referrers:     {0:10}\n".format(num_invalid_urls))
out.write("Avg Graph size:          {0:10}\n".format(num_requests / float(num_graphs - num_non_browser)))
out.write("\n")

out.write("Amazon Stats\n")
out.write("===\n")
out.write("# requests to Amazon:    {0:10}\n".format(num_amazon_requests))
out.write("# Amazon graph roots:    {0:10}\n".format(num_amazon_roots))
out.write("# requests w/o referrer: {0:10}\n".format(num_amazon_no_referrer))
out.write("# Checkouts:             {0:10}\n".format(len(amazon_checkout_graphs)))
out.write("# Sets:                  {0:10}\n".format(len(amazon_stuff_graphs)))
out.write("# Stuffs:                {0:10}\n".format(len(amazon_set_graphs)))
out.write("\n")

out.write("Stuff / Set / Checkout Stats\n")
out.write("===\n")
for marketer_name, histories in history_by_client.items():
    for client_hash, h in histories.items():
        num_stuffs, num_sets, num_carts = h.counts()
        # Only print out graph information here if there is at least
        # two items of interest in the graph (ie a stuff and a set, or
        # something similar)
        if num_stuffs == 0 and num_sets == 0:
            continue

        for c in h.checkouts(seconds=args.secs, cookie_ttl=args.ttl):
            out.write(str(c))
            out.write("\n\n")

out.write("\n")

out.write("Checkout / set / stuff graph Stats\n")
out.write("===\n")
reports = (
    ("Checkouts", amazon_checkout_graphs),
    ("Sets", amazon_set_graphs),
    ("Stuffs", amazon_stuff_graphs)
)
for label, graphs in reports:
    out.write("{0}\n---\n".format(label))
    for g in graphs:
        out.write(str(g))
        out.write("\n\n")
    out.write("\n\n")

out.write("Invalid Looking Referrers\n")
out.write("===\n")
for r in invalid_referrers:
    out.write("{0}\n".format(r))
out.write("\n")


out.write("Unmatched Referrer hosts\n")
out.write("===\n")

sorted_host_index = 0
sorted_hosts = sorted(hosts.iteritems(), key=lambda x: x[1], reverse=True)
# Lets not get crazy, lets only print out the top 50 hosts
for host, count in sorted_hosts[:50]:
    sorted_host_index += 1
    out.write("{0}. {1}: {2}\n".format(sorted_host_index, host, count))

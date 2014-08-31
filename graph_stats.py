"""Checks a collection of bro log graphs to measure two values, 1) how many
graph heads have referrer heads, and 2) what hosts each of those referrer heads
are for."""

import sys
import brotools.reports
import brotools.records
import urlparse
from stuffing.amazon import AmazonAffiliateHistory


parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

debug("Preparing to reading {0} sets of graphs".format(count))
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

num_amazon_requests = 0
num_amazon_roots = 0
num_amazon_no_referrer = 0
num_amazon_checkouts = 0
num_amazon_sets = 0
num_amazon_stuffs = 0

hosts = {}
index = 0
for path, graphs in ins():
    index += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    debug("{0}-{1}. Found {2} graphs".format(index, count, len(graphs)))
    for g in graphs:
        num_requests += len(g)
        num_amazon_requests += sum([len(g.nodes_for_host(h)) for h in amazon_ish_hosts])
        num_amazon_stuffs += len(AmazonAffiliateHistory.stuffs_in_graph(g))
        num_amazon_checkouts += len(AmazonAffiliateHistory.checkouts_in_graph(g))
        num_amazon_sets += len(AmazonAffiliateHistory.cookie_sets_in_graph(g))
        head_record = g._root
        is_amazon_head = "amazon.com" in head_record.host

        if is_amazon_head:
            num_amazon_roots += 1

        if not head_record.referrer:
            num_with_no_referrers += 1
            num_amazon_no_referrer += 1
            continue

        num_unmatched_referrers += 1
        try:
            url_parts = urlparse.urlparse("http://{0}".format(head_record.referrer))
        except ValueError:
            num_invalid_urls += 1
            out.write("\tInvalid referrer: {0}\n".format(head_record.referrer))
            continue
        referrer_host = url_parts.netloc
        try:
            hosts[referrer_host] += 1
        except KeyError:
            hosts[referrer_host] = 1

out.write("General Stats\n")
out.write("===\n")
out.write("# graphs: {0}\n".format(num_graphs))
out.write("# requests: {0}\n".format(num_requests))
out.write("Avg Graph size: {0}\n".format(num_requests / int(num_graphs)))
out.write("# no referrer: {0}\n".format(num_with_no_referrers))
out.write("# unmatched referrer: {0}\n".format(num_unmatched_referrers))
out.write("# invalid referrers: {0}\n".format(num_invalid_urls))
out.write("\n")

out.write("Amazon Stats\n")
out.write("===\n")
out.write("# requests to Amazon: {0}\n".format(num_amazon_requests))
out.write("# Amazon graph roots: {0}\n".format(num_amazon_roots))
out.write("# requests w/o referrer: {0}\n".format(num_amazon_no_referrer))
out.write("# Amazon checkouts: {0}\n".format(num_amazon_checkouts))
out.write("# Amazon sets: {0}\n".format(num_amazon_sets))
out.write("# Amazon stuffs: {0}\n".format(num_amazon_stuffs))
out.write("\n")

out.write("Referrer hosts\n")
out.write("===\n")

sorted_host_index = 0
sorted_hosts = sorted(hosts.iteritems(), key=lambda x: x[1], reverse=True)
# Lets not get crazy, lets only print out the top 250 hosts
for host, count in sorted_hosts[:250]:
    sorted_host_index += 1
    out.write("{0}. {1}: {2}\n".format(sorted_host_index, host, count))

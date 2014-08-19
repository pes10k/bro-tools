"""Checks a collection of bro log graphs to measure two values, 1) how many
graph heads have referrer heads, and 2) what hosts each of those referrer heads
are for."""

import sys
import brotools.reports
import brotools.records
import urlparse


parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

debug("Preparing to reading {0} sets o fgraphs".format(count))
num_records = 0
num_with_no_referrers = 0
num_unmatched_referrers = 0
num_graphs = 0
num_invalid_urls = 0
hosts = {}
index = 0
for path, graphs in ins():
    index += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    debug("{0}-{1}. Found {2} graphs".format(index, count, len(graphs)))
    for g in graphs:
        num_records += len(g)
        head = g._root
        if not head.referrer:
            num_with_no_referrers += 1
            continue
        num_unmatched_referrers += 1
        try:
            url_parts = urlparse.urlparse("http://{0}".format(head.referrer))
        except ValueError:
            num_invalid_urls += 1
            out.write("\tInvalid referrer: {0}\n".format(head.referrer))
            continue
        referrer_host = url_parts.netloc
        try:
            hosts[referrer_host] += 1
        except KeyError:
            hosts[referrer_host] = 1

sorted_hosts = sorted(hosts.iteritems(), key=lambda x: x[1], reverse=True)
out.write("# records found: {0}\n".format(num_records))
out.write("# no referrer: {0}\n".format(num_with_no_referrers))
out.write("# with unmatched referrer: {0}\n".format(num_unmatched_referrers))
out.write("# invalid referrers: {0}\n".format(num_invalid_urls))
out.write("Referrer hosts\n")
for host, count in sorted_hosts:
    out.write("{0}: {1}\n".format(host, count))

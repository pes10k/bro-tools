"""Checks a collection of bro log graphs to measure two values, 1) how many
graph heads have referrer heads, and 2) what hosts each of those referrer heads
are for."""

import sys
import brotools.reports
import brotools.records
import urlparse
import user_agents
from stuffing.amazon import AmazonAffiliateHistory


parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--graphs', '-g', action="store_true",
                    help="If provided, a list of graphs will be included in " +
                    "the report, describing cookie stuffs, etc.")
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

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

if args.graphs:
    amazon_stuff_graphs = []
    amazon_checkout_graphs = []
    amazon_set_graphs = []
    amazon_action_graphs = []
    amazon_high_action_graphs = []
else:
    num_amazon_stuffs = 0
    num_amazon_checkouts = 0
    num_amazon_sets = 0

hosts = {}
invalid_referrers = []
index = 0
for path, graph in ins():
    index += 1
    num_graphs += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    num_requests += len(graph)
    num_amazon_requests += sum([len(graph.nodes_for_host(h)) for h in amazon_ish_hosts])

    if args.graphs:
        local_amazon_stuff_graphs = AmazonAffiliateHistory.stuffs_in_graph(graph)
        local_amazon_checkout_graphs = AmazonAffiliateHistory.checkouts_in_graph(graph)
        local_amazon_set_graphs = AmazonAffiliateHistory.cookie_sets_in_graph(graph)

        local_amazon_action_graphs = (set(local_amazon_stuff_graphs)
            .intersection(set(local_amazon_checkout_graphs)))

        local_amazon_high_action_graphs = (local_amazon_action_graphs
            .intersection(set(local_amazon_set_graphs)))

        amazon_stuff_graphs += local_amazon_stuff_graphs
        amazon_checkout_graphs += local_amazon_checkout_graphs
        amazon_set_graphs += local_amazon_set_graphs
        amazon_action_graphs += local_amazon_action_graphs
        amazon_high_action_graphs += local_amazon_high_action_graphs
    else:
        num_amazon_stuffs += len(AmazonAffiliateHistory.stuffs_in_graph(graph))
        num_amazon_checkouts += len(AmazonAffiliateHistory.checkouts_in_graph(graph))
        num_amazon_sets += len(AmazonAffiliateHistory.cookie_sets_in_graph(graph))

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

if args.graphs:
    out.write("# Checkouts:      {0:10}\n".format(len(amazon_checkout_graphs)))
    out.write("# Sets:           {0:10}\n".format(len(amazon_stuff_graphs)))
    out.write("# Stuffs:         {0:10}\n".format(len(amazon_set_graphs)))
    out.write("# CO + Stuff:     {0:10}\n".format(len(amazon_action_graphs)))
    out.write("# CO + SET + STF: {0:10}\n".format(len(amazon_high_action_graphs)))
else:
    out.write("# Amazon checkouts:      {0:10}\n".format(num_amazon_checkouts))
    out.write("# Amazon sets:           {0:10}\n".format(num_amazon_sets))
    out.write("# Amazon stuffs:         {0:10}\n".format(num_amazon_stuffs))
out.write("\n")

if args.graphs:
    reports = (
        ("Sets, Stuffs & Checkouts", amazon_high_action_graphs),
        ("Stuffs & Checkouts", amazon_action_graphs),
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
# Lets not get crazy, lets only print out the top 250 hosts
for host, count in sorted_hosts[:100]:
    sorted_host_index += 1
    out.write("{0}. {1}: {2}\n".format(sorted_host_index, host, count))

#!/usr/bin/env python
"""Track amazon session tokens across multiple IPs, to see how much
possible Amazon tracking / stuffing we're missing due to IP jumping by clients.
"""

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import brotools.reports
import brotools.records
import stuffing.amazon

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

# This dict will store amazon session tokens as keys,
# and a set of IP addresses (as strings) for each IP that session token
# was sent out from
token_ips = {}

index = 0
debug("Getting ready to start reading {0} graphs".format(count))
for path, graphs in ins():
    index += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    debug("{0}-{1}. Found {2} graphs".format(index, count, len(graphs)))
    for g in graphs:
        nodes = g.nodes_for_hosts("www.amazon.com", "amazon.com")
        for n in nodes:
            token = stuffing.amazon.session_token(n)
            if not token:
                continue
            debug(" * Session token: {0} @ {1}".format(token, n.id_orig_h))
            if token not in token_ips:
                token_ips[token] = set()
            token_ips[token].add(n.id_orig_h)

num_ips = sum([len(ips) for ips in token_ips.values()])

out.write("Session Tokens: {}\n".format(len(token_ips)))
out.write("IPs: {}\n".format(num_ips))
out.write("Avg IPs Per Token: {}\n".format(num_ips / float(len(token_ips))))
out.write("==========\n")
for token, ips in token_ips.items():
    out.write("\n")
    out.write("Token: {}\n".format(token))
    out.write("IPs: {}\n".format(len(ips)))
    out.write("-----\n")
    for ip in ips:
        out.write(" * {}\n".format(ip))

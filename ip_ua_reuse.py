"""Checks to see how often the same ip and user agent appear with different
amazon session tokens, as a loose proxy for how many clients are sharing an ip
behind a NAT."""

import pprint
import re
import sys
import brotools.reports
import brotools.records

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

amz_token_pattern = re.compile('session-token=([^\;]+)')

debug("Getting ready to start reading {0} graphs".format(count))
index = 0

ip_tokens = {}
ip_ua_tokens = {}

for path, graphs in ins:
    index += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    debug("{0}-{1}. Found {2} graphs".format(index, count, len(graphs)))
    for g in graphs:
        nodes = []
        if g.nodes_for_host('www.amazon.com'):
            nodes += g.nodes_for_host('www.amazon.com')
        if g.nodes_for_host("amazon.com"):
            nodes += g.nodes_for_host("amazon.com")
        for n in nodes:
            if not n.cookies:
                continue
            match = amz_token_pattern.search(n.cookies)
            if not match:
                continue
            key = g.ip + " " + n.user_agent
            token = match.group(1)

            if g.ip not in ip_tokens:
                ip_tokens[g.ip] = {}
            if token not in ip_tokens[g.ip]:
                ip_tokens[g.ip][token] = []
            ip_tokens[g.ip][token].append(n.ts)

            if key not in ip_ua_tokens:
                ip_ua_tokens[key] = {}
            if token not in ip_ua_tokens[key]:
                ip_ua_tokens[key][token] = []
            ip_ua_tokens[key][token].append(n.ts)

num_reused_ips = sum([1 if len(v) > 1 else 0 for v in ip_tokens.values()])

out.write("IP Aliasing\n")
out.write("# IPs: {0}\n".format(len(ip_tokens)))
out.write("Reused IPS: {0}\n".format(num_reused_ips))
out.write("----------\n\n")
for ip, tokens in ip_tokens.items():
    out.write("IP: {0}\n".format(ip))
    for t, dates in tokens.items():
        out.write(" * Session Token: {0}\n".format(t))
        for d in dates:
            out.write(" * * {0}\n".format(d))
    out.write("\n")
out.write("\n\n")

num_reused_ip_ua = sum([1 if len(v) > 1 else 0 for v in ip_ua_tokens.values()])
out.write("IP/UA Aliasing\n")
out.write("# IPs / UA Pairs: {0}\n".format(len(ip_ua_tokens)))
out.write("Reused IPS / UA Pairs: {0}\n".format(num_reused_ip_ua))
out.write("----------\n")
for key, tokens in ip_tokens.items():
    parts = key.split(" ")
    ip = parts[0]
    ua = " ".join(parts[1:])
    out.write("IP: {0}\n".format(ip))
    out.write("UA: {0}\n".format(ua))
    for t, dates in tokens.items():
        out.write(" * Session Token: {0}\n".format(t))
        for d in dates:
            out.write(" * * {0}\n".format(d))
    out.write("\n")
out.write("\n\n")

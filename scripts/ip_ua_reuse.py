#!/usr/bin/env python
"""Checks to see how often the same ip and user agent appear with different
amazon session tokens, as a loose proxy for how many clients are sharing an ip
behind a NAT."""

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import brotools.reports
import brotools.records
import stuffing.amazon

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)


def collision(*args):
    """Returns a boolean description of whether there are any overlapping
    dates in the two given, sorted date ranges."""
    date_ranges = sorted(args, key=lambda x: x[0])
    last_date = None
    for a in date_ranges:
        if not last_date:
            last_date = a[-1]
            continue
        if last_date > a[0]:
            return True
        last_date = a[-1]
    return False

debug("Getting ready to start reading {0} graphs".format(count))
index = 0

ip_tokens = {}
ip_ua_tokens = {}

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
            key = g.ip + " " + n.user_agent
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
num_collisions = sum([1 if collision(tokens.values()) else 0 for tokens in [t for t in ip_tokens.values()]])

out.write("IP Aliasing\n")
out.write("# IPs: {0}\n".format(len(ip_tokens)))
out.write("Reused IPS: {0}\n".format(num_reused_ips))
out.write("Collisions: {0}\n".format(num_collisions))
out.write("==========\n\n")
for ip, tokens in ip_tokens.items():
    if len(tokens) == 1:
        continue
    is_collision = collision(tokens.values())
    out.write("IP: {0}\n".format(ip))
    out.write("Collision: {0}\n".format("YES" if is_collision else "NO"))
    out.write("-----\n")
    for t, dates in tokens.items():
        out.write(" * Session Token: {0}\n".format(t))
        for d in dates:
            out.write(" * * {0}\n".format(d))
        out.write("\n")
    out.write("\n")
out.write("\n\n")

num_reused_ip_ua = sum([1 if len(v) > 1 else 0 for v in ip_ua_tokens.values()])
num_collisions = sum([1 if collision(tokens.values()) else 0 for tokens in [t for t in ip_tokens.values()]])

out.write("IP/UA Aliasing\n")
out.write("# IPs / UA Pairs: {0}\n".format(len(ip_ua_tokens)))
out.write("Reused IPS / UA Pairs: {0}\n".format(num_reused_ip_ua))
out.write("Collisions: {0}\n".format(num_collisions))
out.write("==========\n\n")
for key, tokens in ip_ua_tokens.items():
    if len(tokens) == 1:
        continue
    parts = key.split(" ")
    ip = parts[0]
    ua = " ".join(parts[1:])
    is_collision = collision(tokens.values())
    out.write("IP: {0}\n".format(ip))
    out.write("UA: {0}\n".format(ua))
    out.write("Collision: {0}\n".format("YES" if is_collision else "NO"))
    out.write("-----\n")
    for t, dates in tokens.items():
        out.write(" * Session Token: {0}\n".format(t))
        for d in dates:
            out.write(" * * {0}\n".format(d))
        out.write("\n")
    out.write("\n")
out.write("\n\n")

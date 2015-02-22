#!/usr/bin/env python
"""Commandline utility for examining pickled collections of BroRecordGraph
objects and looking for instances where cookie stuffing "stole" a
credit from someone who looks like a valid cookie setter."""

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import brotools.reports
import csv

parser = brotools.reports.marketing_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--ttl', type=int, default=84600,
                    help="The time, in seconds, that an Amazon set affiliate "
                    "marketing cookie is expected to be valid.  Default is "
                    "one day (84600 seconds)")
parser.add_argument('--secs', type=int, default=3600,
                    help="The minimum time in seconds that must pass between "
                    "a client's requests to the marketers 'add to cart' page "
                    "for those requests to be treated as a seperate checkout")
cli_params = brotools.reports.parse_marketing_cli_args(parser)
count, ins, out, debug, marketers, args = cli_params

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

# Marketer Name -> set(<found cookie values>)
session_cookies = {}

# Marketer Name -> # sets
cookie_set_counts = {}

# Marketer Name -> # stuffs
cookie_stuff_counts = {}

# Marketer Name -> # checkouts
checkout_counts = {}

# Marketer Name -> #
stuffed_purchase_counts = {}

# Marketer Name -> #
valid_purchase_counts = {}

# Marketer Name -> #
stolen_purchase_counts = {}

# Markter Name -> #
request_counts = {}

# Markter Name -> set(<partner tags>)
partner_tags = {}

index = 0
old_path = None
debug("Preparing to start reading {0} pickled data".format(count))
marketer_lookup = {}

for path, g in ins():
    if not old_path or old_path != path:
        index += 1
        old_path = path
        debug("{0}-{1}. Considering {2}".format(index, count, path))
    for marketer in marketers:

        if marketer.name() not in marketer_lookup:
            marketer_lookup[marketer.name()] = marketer
            session_cookies[marketer.name()] = set()
            cookie_set_counts[marketer.name()] = 0
            cookie_stuff_counts[marketer.name()] = 0
            checkout_counts[marketer.name()] = 0
            stuffed_purchase_counts[marketer.name()] = 0
            valid_purchase_counts[marketer.name()] = 0
            stolen_purchase_counts[marketer.name()] = 0
            request_counts[marketer.name()] = 0
            partner_tags[marketer.name()] = set()

        request_counts[marketer.name()] += len(marketer.nodes_for_domains(g))

        if len(marketer.stuffs_in_graph(g)) > 0:
            cookie_stuff_counts[marketer.name()] += 1

        if len(marketer.cookie_sets_in_graph(g)) > 0:
            cookie_set_counts[marketer.name()] += 1

        # See if we can find a session tracking cookie for this visitor
        # in this graph.  If not, then we know there are no cookie stuffs,
        # checkouts, or other relevant activity in the graph we
        # care about, so we can continue
        hash_key = marketer.session_id_for_graph(g)
        if not hash_key:
            continue

        referrer_tag = marketer.get_referrer_tag(g)
        if referrer_tag:
            partner_tags[marketer.name()].add(referrer_tag)

        session_cookies[marketer.name()].add(hash_key)

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
            history.consider(g)
        except KeyError:
            history = marketer(g)
            client_dict[hash_key] = history

checkout_count = 0
stuffs_count = 0
sets_count = 0
parties_count = 0
steals_count = 0
conservative_stuffs_count = 0

for marketer_name, histories in history_by_client.items():
    marketer = marketer_lookup[marketer_name]
    for client_hash, h in histories.items():
        for c in h.checkouts(seconds=args.secs, cookie_ttl=args.ttl):
            checkout_counts[marketer_name] += 1

            if c.is_purchase_stuffed():
                stuffed_purchase_counts[marketer_name] += 1

            if c.is_purchase_with_valid_cookie():
                valid_purchase_counts[marketer_name] += 1

            if c.is_stolen_pruchase():
                valid_purchase_counts[marketer_name] += 1


names = sorted(valid_purchase_counts.keys())
columns = (
    ("Affiliate", names),
    ("# Requests", request_counts),
    ("# AMs", {m: len(partner_tags[m]) for m in names}),
    ("# Tracking Cookies", {m: len(session_cookies[m]) for m in names}),
    ("# Cookie Sets", cookie_set_counts),
    ("# Cookie Stuffs", cookie_stuff_counts),
    ("# Checkouts", checkout_counts),
    ("Purchases credited to valid cookie", valid_purchase_counts),
    ("Purchases credited to a stuffed cookie", stuffed_purchase_counts),
    ("'Stolen' purchases", stolen_purchase_counts),
)

writer = csv.writer(out)
writer.writerow([h for h, v in columns])
for name in names:
    row = [name] + [v[name] for h, v in columns[1:]]
    writer.writerow(row)

totals_row = ["Total"] + [sum(v.values()) for h, v in columns[1:]]
writer.writerow(totals_row)

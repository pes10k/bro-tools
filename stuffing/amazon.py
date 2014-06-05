"""Tools for looking for instances of amazon cookie stuffing in individual
and collections of BroRecords."""

import re
import urlparse

AMZ_COOKIE_URL = re.compile(r'(?:&|\?|^)tag=')

def is_cookie_set(br):
    """Returns a boolean description of whether the given BroRecord looks like
    it would cause an affiliate cookie to be set on the visiting browser.

    Args:
        br -- a BroRecord

    Return:
        True if it looks like the given request would cause an amazon affiliate
        cookie to be set, and otherwise False.
    """
    if br.host not in ("amazon.com", "www.amazon.com"):
        return False

    q = urlparse.urlparse(br.uri).query
    if not AMZ_COOKIE_URL.search(q):
        return False

    return True

def stuffs_in_graph(graph, time=2, sub_time=2):
    """Returns a list of all nodes in a given BroRecordGraph that are suspected
    cookie stuffing attempts.

    Args:
        graph -- a BroRecordGraph instance

    Keyword Args:
        time     -- the maximum amount of time that can pass between a node
                    and the node that referred it for it to still count as
                    a stuffing attempt
        sub_time -- the maximum amount this section of the browsing graph
                    can be active after this request without disqualifying it
                    as a cookie stuffing instance

    Return:
        A list of zero or more BroRecord instances
    """
    nodes = []
    for node in (n for n in graph.nodes() if is_cookie_set(n)):

        parent = graph.parent_of_node(node)
        if not parent:
            continue

        time_diff = parent.ts - node.ts
        if time_diff > time:
            continue

        if graph.remaining_child_time(node) > sub_time:
            continue

        nodes.append(node)

    return nodes

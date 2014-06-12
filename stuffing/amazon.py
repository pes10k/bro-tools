"""Tools for looking for instances of amazon cookie stuffing in individual
and collections of BroRecords."""

import re
import urlparse

AMZ_COOKIE_URL = re.compile(r'(?:&|\?|^)tag=')
AMAZON_DOMAINS = ("amazon.com", "www.amazon.com")

def referrer_tag(br):
    """Returns the amazon affiliate marketing tag in the bro record request,
    if it exists.

    Args:
        br -- a BroRecord

    Return:
        The affiliate marketing tag, if it exists in the bro record, otherwise
        None.
    """
    query_params = br.query_params()
    try:
        tags = query_params['tag']
        return None if len(tags) > 0 else tags[0]
    except KeyError:
        return None

def is_add_to_cart(br):
    """Returns a boolean description of whether a given BroRecord appears to
    be a request to add an item to amazon shopping cart.

    Args:
        br -- a BroRecord

    Return:
        True if it looks a request to add an item to a shopping cart,
        and otherwise False.
    """
    if br.host not in AMAZON_DOMAINS:
        return False

    if 'handle-buy-box' not in br.uri:
        return False

    return True

def is_cookie_set(br):
    """Returns a boolean description of whether the given BroRecord looks like
    it would cause an affiliate cookie to be set on the visiting browser.

    Args:
        br -- a BroRecord

    Return:
        True if it looks like the given request would cause an amazon affiliate
        cookie to be set, and otherwise False.
    """
    if br.host not in AMAZON_DOMAINS:
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
    stuffing_nodes = []
    amazon_nodes = []

    for d in AMAZON_DOMAINS:
        nodes_for_d = graph.nodes_for_host(d)
        if nodes_for_d:
            amazon_nodes += nodes_for_d

    for node in (n for n in amazon_nodes if is_cookie_set(n)):

        parent = graph.parent_of_node(node)
        if not parent:
            continue

        time_diff = parent.ts - node.ts
        if time_diff > time:
            continue

        if graph.remaining_child_time(node) > sub_time:
            continue

        stuffing_nodes.append(node)

    return stuffing_nodes

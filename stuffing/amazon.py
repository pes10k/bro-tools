"""Tools for looking for instances of amazon cookie stuffing in individual
and collections of BroRecords."""

import re
import urlparse

AMZ_COOKIE_URL = re.compile(r'(?:&|\?|^)tag=')
AMAZON_DOMAINS = ("amazon.com", "www.amazon.com")

# Values used for tracking whether a given bro record represents a cookie
# stuffing incident (STUFF), a seemingly valid one (SET), or a request
# to add an item to a users Amazon cart
STUFF = 0
SET = 1
CART = 2

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

def cart_adds_in_graph(graph):
    """Returns a list of zero or more BroRecords in the given BroRecordGraph
    that look like a request to amazon to add an item to the user's shopping
    cart.

    Args:
        graph -- a BroRecordGraph instance

    Return:
        A list of zero or more BroRecords
    """
    amazon_nodes = []
    for d in AMAZON_DOMAINS:
        nodes_for_d = graph.nodes_for_host(d)
        if nodes_for_d:
            amazon_nodes += nodes_for_d
    return [n for n in amazon_nodes if is_add_to_cart(n)]

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

def cookie_sets_in_graph(graph):
    """Returns a list of all nodes in a given BroRecordGraph that look
    like they would cause an affiliate marketing cookie to be set.

    Args:
        graph -- a BroRecordGraph instance

    Return:
        A list of zero or more BroRecords
    """
    amazon_nodes = []
    for d in AMAZON_DOMAINS:
        nodes_for_d = graph.nodes_for_host(d)
        if nodes_for_d:
            amazon_nodes += nodes_for_d
    return [n for n in amazon_nodes if is_cookie_set(n)]

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

    for node in cookie_sets_in_graph(graph):
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


class AmazonHistory(object):
    """Stores a history of a clients interactions with amazon.  Well, not
    all interactions, just affiliate marketing cookie sets (and stuffs),
    and hitting the shopping cart.
    """

    def __init__(self, graph):
        """The initializer takes an initial graph, which will be parsed to
        create the initial set of history points in this collection.

        Args:
            graph -- a BroRecordGraph instance
        """
        self.ip = graph.ip
        self.user_agent = graph.user_agent

        # Each of these lists will contain tuples of two values,
        # (record, graph), the BroRecord of the actual request, and
        # the BroRecordGraph that the request came from
        self._cookie_stuffs = []
        self._cookie_sets = []
        self._cart_requests = []
        self.consider(graph)

    def consider(self, graph):
        """Examines a given graph of web requests, and extracts the
        relevant points were interested in tracking (instances of cookie
        stuffing, legitimate-seeming affiliate cookie setting, and
        additions to the amazon shopping cart.

        Args:
            graph -- a BroRecordGraph instance

        Return:
            A tuple of three values, the counts of the number of
            cookie stuffs, legit-seeming cookie stuffs, and cart requests
            found in the given graph.
        """
        stuff_nodes = stuffs_in_graph(graph)
        cookie_set_nodes = cookie_sets_in_graph(graph)

        # Remove all cookie *stuff* nodes that also appear in the cookie *set*
        # nodes collection, since the former will be a subset of the latter
        for n in stuff_nodes:
            if (n, graph) in cookie_set_nodes:
                cookie_set_nodes.remove((n, graph))

        cart_adds = cart_adds_in_graph(graph)

        self._cookie_stuffs += [(n, graph) for n in stuff_nodes]
        self._cookie_sets += [(n, graph) for n in cookie_set_nodes]
        self._cart_requests += [(n, graph) for n in cart_adds]

        return len(stuff_nodes), len(cookie_set_nodes), len(cart_adds)

    def counts(self):
        """Returns a brief summary of the number of relevant requests currently
        in the collection.

        Return:
            A tuple of three values, the counts of the number of
            cookie stuffs, legit-seeming cookie stuffs, and cart requests
            found in the given graph.
        """
        return (len(self._cookie_stuffs), len(self._cookie_sets),
                len(self._cart_requests))

    def prune(self, seconds=3600):
        """Prunes the cart add requests to remove additions to the cart that
        happen very close to each other, to avoid double counting instances
        of checkouts.  Closely occurring requests will be removed.

        Time is considered from the first request, not each subsequent request.
        So if request 1 happens at 1:00AM, request 2 happens 1:30AM and request
        3 happens at 2:30AM, and seconds is set to 3600, request 2 will
        be removed from the collection.

        Keyword Args:
            seconds -- the maximum number of seconds that can occur between
                       a cart addition and it still be removed. Defaults to
                       one hour.

        Return:
            The number of cart additions that were pruned from the collection.
        """
        self._cart_requests.sort(key=lambda x: x[0].ts)
        removed_count = 0
        last_kept_request = None
        for v in self._cart_requests:
            r, g = v
            if not last_kept_request:
                last_kept_request = r
                continue

            if r.ts - last_kept_request.ts < seconds:
                self._cart_requests.remove(v)
                removed_count += 1
                continue

            last_kept_request = r

        return removed_count

    def checkouts(self, seconds=3600, cookie_ttl=84600):
        """Returns a list of AmazonCheckout instances, describing each of
        the times this client checked out at Amazon.

        Keyword Args:
            seconds    -- the maximum number of seconds that can occur between
                          a cart addition and it still be removed. Defaults to
                          one hour.
            cookie_ttl -- the maximum amount of time represented by this
                          checkout history, which should correspond to
                          the expected TTL of an amazon affiliate cookie.
                          Defaults to 1 day (84600 seconds).

        Return:
            A list of zero or more AmazonCheckout instances.
        """
        # First, remove redundant amazon checkout records from the collection.
        # Since we're only tracking add-to-cart instances, there may be
        # more than one add-to-cart record corresponding to each actual
        # checkout event.  This pruning step is intended to reduce the number
        # of false positives this add-to-cart-as-proxy-for-checkout method
        # would introduce
        self.prune(seconds=seconds)

        # Now combine all of the types of records we're tracking
        # (add to cart requests, cookie stuffs, and cookie setting
        # requests) together in a single collection, and then sort them
        # in reverse order, from latest occurring to most recently occurring,
        # so that we can walk through them once to build up the history
        # of each checkout instance
        requests = []
        requests += [(r, g, CART) for r, g in self._cart_requests]
        requests += [(r, g, STUFF) for r, g in self._cookie_stuffs]
        requests += [(r, g, SET) for r, g in self._cookie_sets]
        requests.sort(key=lambda x: x[0].ts, reverse=True)

        checkouts = []
        for r, g, t in requests:
            # If the current record is a request to add something to
            # the amazon cart, then automatically start a new checkout
            # collection to track what happens to this this request
            if t == CART:
                checkouts.append(AmazonCheckout(r, g, cookie_ttl))
                continue

            # Otherwise, if this is not a request to add something to a cart,
            # but we haven't seen any requests to add an item to a cart,
            # then there is no checkout instance to be possibly affected
            # by the cookie this request is setting, so safely ignore
            if len(checkouts) == 0:
                continue

            # Otherwise, we just attempt to add either a cookie set or
            # cookie stuffing instance to the current checkout history
            # collection.  Note that these requests will fail (ie just do
            # nothing) if they occurred too long after the most recent
            # checkout / cart-add request, and so any cookie they were setting
            # would be void
            if t == SET:
                checkouts[-1].add_cookie_set(r, g)
            else:
                checkouts[-1].add_cookie_stuff(r, g)
        return checkouts


class AmazonCheckout(object):
    """Represents the history of a (suspected) checkout on amazon, where
    a request to add an item to a shopping cart is taken as a proxy for
    actually checking out.  This class is intended to make it easier to
    track instances of where someone making a purchase on amazon was a 'victim'
    of cookie stuffing.

    There is probably no reason to instantiate instances of this class directly.
    Instead, really only makes sense for AmazonHistory instances to generate
    them.
    """

    def __init__(self, record, graph, cookie_ttl=84600):
        """Initializer requires a reference to a BroRecord that represents
        an amazon checkout / cart add, and the BroRecordGraph instance that
        checkout occurred in.

        Args:
            record     -- a BroRecord
            graph      -- a BroRecordGraph that contains the BroRecord

        Keyword Args:
            cookie_ttl -- the maximum amount of time represented by this
                          checkout history, which should correspond to
                          the expected TTL of an amazon affiliate cookie.
                          Defaults to 1 day (84600 seconds).
        """
        self.cookie_ttl = cookie_ttl
        self.ts = record.ts

        self.ip = graph.ip
        self.user_agent = graph.user_agent

        self.cart_record = record
        self.cart_graph = graph

        # Both of the below lists store tuples of two values,
        # (BroRecord, BroRecordGraph), or a request, and the request graph
        # that request came from
        self._cookie_sets = []
        self._cookie_stuffs = []

        # Flag for whether the current compiled values in the class are correct
        # (_dirty = False) or out of sync (_dirty = True)
        self._dirty = True

        # A sorted list of tuples of the form (BroRecord, BroRecordGraph),
        # with the request happening closest in time to the checkout request
        # occurring at position 0, and the oldest request being at position -1.
        # In order to avoid needing to do redundant sorts, the correctness
        # of this value is tracked by the _dirty flag
        self._history = None

    def __str__(self):
        output = "IP: {0}\n".format(self.ip)
        output += "Agent: {0}\n".format(self.user_agent)
        output += "Checkout Time: {0}\n".format(self.cart_record.date_str)
        output += "\n"
        output += "History\n"
        output += "--------------------\n"
        for r, g, t in self.cookie_history():
            type_str = "STUFF" if t == STUFF else "SET  "
            output += "{0} {1} {2} {3}\n".format(type_str, r.date_str,
                                                 referrer_tag(r), r.ur)
        return output

    def add_cookie_set(self, record, graph):
        """Adds an instance of a legit seeming affiliate cookie setting record
        to the history of this amazon checkout.

        Args:
            record -- a BroRecord
            graph  -- a BroRecordGraph containing the given record

        Return:
            False if this cookie this request set would not have not have
            been valid at the time of checkout, and thus the passed
            record is not accepted into the history, or otherwise True.
        """
        if not self._is_request_in_window(record):
            return False

        # Otherwise, the cookie setting request validly falls into the
        # set of requests that influence which affiliate cookie was present
        # at the time of the checkout / cart add
        self._dirty = True
        self._cookie_sets.append((record, graph))

    def add_cookie_stuff(self, record, graph):
        """Adds an instance of a suspected cookie stuffing to the history of
        this amazon checkout.

        Args:
            record -- a BroRecord
            graph  -- a BroRecordGraph containing the given record

        Return:
            False if this cookie this request set would not have not have
            been valid at the time of checkout, and thus the passed
            record is not accepted into the history, or otherwise True.
        """
        if not self._is_request_in_window(record):
            return False

        # Otherwise, the cookie setting request validly falls into the
        # set of requests that influence which affiliate cookie was present
        # at the time of the checkout / cart add
        self._dirty = True
        self._cookie_stuffs.append((record, graph))

    def had_cookie(self):
        """Returns a boolean description of whether it looks like the amazon
        checkout request happened with an affiliate marketing cookie in place.

        Return:
            The affiliate marketing cookie that was in place at the time of
            checkout, or None if no cookie was in place at the time.
        """
        h = self.cookie_history()
        if len(h) == 0:
            return None
        else:
            return referrer_tag(h[0][0])

    def is_stuffed(self):
        """Returns a boolean description of whether it looks like this
        checkout happened with a stuffed affiliate marketing cookie in place.

        Return:
            True if this checkout appears to have been affected by cookie
            stuffing.
        """
        h = self.cookie_history()
        if len(h) == 0:
            return False
        else:
            record, graph, t = h[0][0]
            return t == STUFF

    def cookie_history(self):
        """Returns a list of requests that could set affiliate marketing cookies
        that were in place at the time of this checkout.  The returned values
        are in reverse order, with the latest occurring (ie the one most likely
        to determine the cookie in place at time of checkout) in the first
        position.

        Return:
            A list of tuples.  Each tuple has three values, 1) a BroRecord that
            that looks like it set an affiliate marketing cookie on the client,
            2) the BroRecordGraph that contains this BroRecord instance, and
            3) either STUFF or SET, depending on whether this request appears
            to be a cookie stuffing instance or a valid cookie setting instance.
        """
        if self._dirty:
            self._history = []
            self._history += [(r, g, STUFF) for r, g in self._cookie_stuffs]
            self._history += [(r, g, SET) for r, g in self._cookie_sets]
            self._history.sort(key=lambda x: x[0].ts, reverse=True)
            self._dirty = False
        return self._history

    def _is_request_in_window(self, record):
        """Checks to see whether the given cookie setting request / BroRecord
        falls in the window of time where the cookie set could be present
        at the time of checkout.

        Args:
            record -- a BroRecord

        Return:
            False if this cookie this request set would not have not have
            been valid at the time of checkout, and thus the passed
            record is not accepted into the history, or otherwise True.
        """
        # First check that the cookie setting request happened before the
        # checkout request.  If the cookie was set afterwards, than trivially
        # the cookie could not have been used when making this purchase
        if record.ts > self.ts:
            return False

        # Next, also check to make sure that the cookie set by this request
        # wouldn't have expired by time of checkout.
        if record.ts + self.cookie_ttl < self.ts:
            return False

        return True

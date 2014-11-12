"""Functions and helpers for gathering the features used in training the
NN classifer.  These functions are used for populating the SQLite3
database that contains information for each graph and domain found in
the stuffing data.
"""
import ssl
import socket
import os
import whois
import requests
import datetime
from dateutil.relativedelta import relativedelta
import sys
import os.path
import re

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import contrib.pagerank
from stuffing.amazon import AmazonAffiliateHistory

CA_BUNDLE = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                         "..", "contrib", "ca-bundle.crt")
CA_BUNDLE = os.path.realpath(CA_BUNDLE)


TITLE_PATTERN = re.compile(r'<title>(.*?)</title>', re.I | re.U)

def page_title(url):
    """Returns the HTML title of the given URL, if one exists.
    This isn't directly mapable onto a training feature, but is useful
    for quickly labling information.

    Args:
        url -- A valid URL as a string, such as http://example.org/resource

    Return:
        The contents of the <title> tag in the HTML, if the URL is reachable
        and the returned resource contains a title tag.  Otherwise, None.
    """
    rs = requests.get(url)
    if not rs:
        return None

    status = rs.status_code
    if status < 200 or status >= 300:
        return None

    body = rs.text
    if not body:
        return None

    match = TITLE_PATTERN.search(body)
    if not match:
        return None

    return match.group(1)


def fetch_cert(domain, port=443, ca_bundle_path=None):
    """Checks to see if the given domain has a x509 cert at all.

    Args:
        domain -- a string, representing a valid domain, such as
                  example.org

    Keyword Args:
        port           -- The port to query on the given domain to check for
                          an advertised cert.
        ca_bundle_path -- A path to a bundle of CA information, in PEM format.
                          If this is not provided, the included CA bundle,
                          in ../contrib/ca-bundle.crt will be used.

    Return:
        A cert object, returned from ssl, if the domain has any kind of
        x509 cert information, and otherwise None.
    """
    if not ca_bundle_path:
        ca_bundle_path = CA_BUNDLE
    try:
        cert = ssl.get_server_certificate((domain, port),
                                          ssl_version=ssl.PROTOCOL_TLSv1,
                                          ca_certs=ca_bundle_path)
        return cert
    except:
        return None


def whois_for_domain(domain):
    """Returns whois information for a given domain, if it exists.

    Args:
        domain -- a string, representing a valid domain, such as
                  example.org

    Return:
        A whois.parser.WhoisCom object describing the whois response,
        if the domain is currently registered.  Otherwise, None.
    """
    try:
        query = whois.whois(domain)
    except whois.parser.PywhoisError:
        return None
    except socket.error:
        return None
    return query


def years_for_domain(whois_rec):
    """Returns the number of years a given domain's registration is
    for.

    Args:
        whois_rec -- a whois.parser.WhoisCom describing a whois
                     record for the given domain

    Return:
        An integer number of years the registration was for.
    """
    def extract_date(date_key):
        a_date = getattr(whois_rec, date_key)
        if isinstance(a_date, datetime.datetime):
            return a_date
        elif isinstance(a_date, list) and len(a_date) > 0:
            return a_date[0]
        else:
            return None
    reg_date = extract_date('creation_date')
    exp_date = extract_date('expiration_date')
    delta = relativedelta(exp_date, reg_date)
    return delta.years


def is_url_live(url):
    """Checks to see whether a given URL is live and valid,
    and returns some non-error HTTP code.

    Args:
        url -- a url, as a string, such as http://example.org

    Return:
        True if the url is reachable without an error.
    """
    try:
        result = requests.get(url)
        status_code = result.status_code
        return (status_code >= 200 and status_code < 300)
    except:
        return False


def page_rank(url):
    """Returns the PageRank score for the given domain.

    Args:
        url -- A valid url, as a string

    Return:
        An integer value, of the page rank score of the url.
    """
    ranker = contrib.pagerank.GooglePageRank()
    return ranker.get_rank(url)


def alexia_rank(url):
    """Returns the Alexa traffic score for the given domain.

    Args:
        url -- A valid url, as a string

    Return:
        An integer value of the alexia traffic score for the url.
    """
    ranker = contrib.pagerank.AlexaTrafficRank()
    return ranker.get_rank(url)


def amazon_time_from_referrer(graph):
    """Finds the amount of time that passed between each amazon cookie
    set in the graph and the referring page being loaded, and returns
    the maximum found time in seconds.

    Args:
        graph -- a BroRecordGraph instance

    Return:
        The maximum amount of time that passed between a referring page
        being loaded and the following amazon cookie setting request, or
        if None if no such times can be calculated (such as there being no
        referrer for all amazon cookie setting requests).
    """
    times = []
    for child in AmazonAffiliateHistory.cookie_sets_in_graph(graph):
        parent = graph.parent_of_node(child)
        if not parent:
            continue

        # Domains cannot stuff themselves, and below check is just
        # a simple way to make sure neither is a subdomain of the other
        if parent.host in child.host or child.host in parent.host:
            continue

        time_diff = child.ts - parent.ts
        times.append(time_diff)
    return max(times) if len(times) > 0 else None


def affiliate_tag_for_cookie_set(graph):
    """Returns the amazon affiliate marketer tag for the first cookie
    set seen in the graph.

    Args:
        graph -- a BroRecordGraph instance

    Return:
        An affiliate marketing tag, or None if none could be found.
    """
    for child in AmazonAffiliateHistory.cookie_sets_in_graph(graph):
        a_tag = AmazonAffiliateHistory.get_referrer_tag(child)
        if a_tag:
            return a_tag
    return None


def amazon_time_after_cookie_set(graph):
    """Returns the maximum amount of time that was spent in this
    graph browsing from a child request to an Amazon cookie setting
    request.

    Args:
        graph -- a BroRecordGraph instance

    Return:
        None of no cookies stuffs could be found, otherwise the
        maximum amount of time spent browsing from a an amazon cookie stting
        request.
    """
    times = []
    for node in AmazonAffiliateHistory.cookie_sets_in_graph(graph):
        parent = graph.parent_of_node(node)
        if not parent:
            continue

        # Domains cannot stuff themselves, and below check is just
        # a simple way to make sure neither is a subdomain of the other
        if parent.host in node.host or node.host in parent.host:
            continue

        times.append(graph.remaining_child_time(node))
    return max(times) if len(times) > 0 else None

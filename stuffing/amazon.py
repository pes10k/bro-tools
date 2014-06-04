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

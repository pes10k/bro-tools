"""Tools for looking for instances of godaddy cookie stuffing in individual
and collections of BroRecords."""

import re
import new
from .affiliate import AffiliateHistory, FULL_DOMAIN, domain_to_class_name

DOMAINS = (
    ("imlive.com", "savebillclickout.ashx", r'spvdr=([^\;]+)'),
    ("sexier.com", "buycredit", r'vi=([^\;]+)'),
    ("www.fetishgalaxy.com", "buycredit", r'vi=([^\;]+)'),
    ("www.shemale.com", "buycredit", r'vi=([^\;]+)'),
    ("www.supermen.com", "buycredit", r'vi=([^\;]+)'),
    ("phonemates.com", "Services/ControlLoader.ashx", r'vi=([^\;]+)'),
    ("wildmatch.com", "join", r'vi=([^\;]+)'),
    ("bangmatch.com", "join", r'vi=([^\;]+)'),
)

SHARED_PATTERN = re.compile(r'(?:&|\?|^|;)wid=', re.I)


class PussyCashAffiliateHistory(AffiliateHistory):

    # Set by the dynamically created sublcasses
    _TOKEN_PATTERN = None
    _CHECKOUT_URL = None
    _DOMAIN = None
    _NAME = None

    @classmethod
    def session_id(cls, record):
        if not record.cookies:
            return None

        match = cls._TOKEN_PATTERN.search(record.cookies)
        if not match:
            return None

        return match.group(1)

    @classmethod
    def checkout_urls(cls):
        """Returns a list of strings, each of which, if found in a url
        on the current marketer, would count as a checkout attempt.  So,
        for example, returning "add-to-cart" would cause a request to
        "example.org/shopping/add-to-cart/item" to count as a checkout
        attempt.

        Return:
            A tuple or list of zero or more strings
        """
        return (cls._CHECKOUT_URL,)

    @classmethod
    def referrer_tag(cls, record):
        return "wid" if "wid=" in record.uri else "WID"

    @classmethod
    def cookie_set_pattern(cls):
        return SHARED_PATTERN

    @classmethod
    def domains(cls):
        return cls._DOMAIN

    @classmethod
    def name(cls):
        return cls._NAME

CLASSES = []
for domain, url, pattern in DOMAINS:
    domain_class_name = domain_to_class_name(domain)
    a_class_name = "PussyCash{0}AffiliateHistory".format(domain_class_name)
    a_class = new.classobj(a_class_name, (PussyCashAffiliateHistory,), {})
    a_class._DOMAIN = [(domain, FULL_DOMAIN)]
    a_class._NAME = "PussyCash Affiliate: {0}".format(domain)
    a_class._CHECKOUT_URL = url
    a_class._TOKEN_PATTERN = re.compile(pattern)
    CLASSES.append(a_class)

"""Tools for looking for instances of godaddy cookie stuffing in individual
and collections of BroRecords."""

import re
import new
from .affiliate import AffiliateHistory, FULL_DOMAIN, domain_to_class_name

DOMAINS = {
    "imlive.com": "savebillclickout.ashx",
    "sexier.com": "buycredit",
    "www.fetishgalaxy.com": "buycredit",
    "www.shemale.com": "buycredit",
    "www.supermen.com" : "buycredit",
    "phonemates.com": "Services/ControlLoader.ashx",
    "wildmatch.com": "join",
    "bangmatch.com": "join"
}

SHARED_PATTERN = re.compile(r'(?:&|\?|^|;)wid=', re.I)


class PussyCashAffiliateHistory(AffiliateHistory):

    # Set by the dynamically created sublcasses
    _CHECKOUT_URL = None
    _DOMAIN = None
    _NAME = None

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
        return cls._CHECKOUT_URL

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
        return "PussyCash Affiliate"

CLASSES = []
for domain, url in DOMAINS.items():
    domain_class_name = domain_to_class_name(domain)
    a_class_name = "PussyCash{0}AffiliateHistory".format(domain_class_name)
    a_class = new.classobj(a_class_name, (PussyCashAffiliateHistory,), {})
    a_class._DOMAIN = [(domain, FULL_DOMAIN)]
    a_class._NAME = "PuussyCash Affiliate: {0}".format(domain)
    a_class._CHECKOUT_URL = url
    CLASSES.append(a_class)

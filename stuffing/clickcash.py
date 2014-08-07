"""Tools for looking for instances of godaddy cookie stuffing in individual
and collections of BroRecords."""

import re
from .affiliate import AffiliateHistory, FULL_DOMAIN

class ClickCashAffiliateHistory(AffiliateHistory):

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
        return (

        )

    @classmethod
    def referrer_tag(cls, record):
        return "wid" if "wid=" in record.uri else "WID"

    @classmethod
    def cookie_set_pattern(cls):
        try:
            return cls._cookie_set_pattern
        except AttributeError:
            cls._cookie_set_pattern = re.compile(r'(?:&|\?|^|;)wid=', re.I)
            return cls._cookie_set_pattern

    @classmethod
    def domains(cls):
        return [
            ("www.model-perfect.com", FULL_DOMAIN),
            ("www.ifriends.net", FULL_DOMAIN),
            ("www.incrediblecams.com", FULL_DOMAIN),
            ("www.sizzlingcams.com", FULL_DOMAIN),
            ("www.webcamdating.us", FULL_DOMAIN),
            ("www.babefever.net", FULL_DOMAIN),
            ("www.model-perfect.com", FULL_DOMAIN)
        ]

    @classmethod
    def name(cls):
        return "ClickCash Affiliate"

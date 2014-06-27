"""Tools for looking for instances of amazon cookie stuffing in individual
and collections of BroRecords."""

import re
from .affiliate import AffiliateHistory, FULL_DOMAIN

class AmazonAffiliateHistory(AffiliateHistory):
    """Subclass of Affiliate History used for tracking client interactions
    with amazon affiliates.
    """

    @classmethod
    def checkout_urls(cls):
        return ('handle-buy-box',)

    @classmethod
    def referrer_tag(cls, record):
        return 'tag'

    @classmethod
    def cookie_set_pattern(cls):
        try:
            return cls._cookie_set_pattern
        except AttributeError:
            cls._cookie_set_pattern = re.compile(r'(?:&|\?|^)tag=')
            return cls._cookie_set_pattern

    @classmethod
    def domains(cls):
        return (("amazon.com", FULL_DOMAIN), ("www.amazon.com", FULL_DOMAIN))

    @classmethod
    def name(cls):
        return "Amazon Affiliate"

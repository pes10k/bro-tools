"""Tools for looking for instances of amazon cookie stuffing in individual
and collections of BroRecords."""

import re
from cached_property import cached_property
from .affiliate import AffiliateHistory, FULL_DOMAIN

class AmazonAffiliateHistory(AffiliateHistory):
    """Subclass of Affiliate History used for tracking client interactions
    with amazon affiliates.
    """

    @classmethod
    @cached_property
    def checkout_urls(cls):
        return ('handle-buy-box',)

    @classmethod
    @cached_property
    def referrer_tag(cls, record):
        return 'tag'

    @classmethod
    @cached_property
    def cookie_set_pattern(cls):
        return re.compile(r'(?:&|\?|^)tag=')

    @classmethod
    @cached_property
    def domains(cls):
        return (("amazon.com", FULL_DOMAIN), ("www.amazon.com", FULL_DOMAIN))

    @classmethod
    @cached_property
    def name(cls):
        return "Amazon Affiliate"

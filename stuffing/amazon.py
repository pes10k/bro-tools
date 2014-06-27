"""Tools for looking for instances of amazon cookie stuffing in individual
and collections of BroRecords."""

import re
from cached_property import cached_property
from .affiliate import AffiliateHistory, FULL_DOMAIN

class AmazonAffiliateHistory(AffiliateHistory):
    """Subclass of Affiliate History used for tracking client interactions
    with amazon affiliates.
    """

    @cached_property
    @classmethod
    def checkout_urls(cls):
        return ('handle-buy-box',)

    @cached_property
    @classmethod
    def referrer_tag(cls, record):
        return 'tag'

    @cached_property
    @classmethod
    def cookie_set_pattern(cls):
        return re.compile(r'(?:&|\?|^)tag=')

    @cached_property
    @classmethod
    def domains(cls):
        return (("amazon.com", FULL_DOMAIN), ("www.amazon.com", FULL_DOMAIN))

    @cached_property
    @classmethod
    def name(cls):
        return "Amazon Affiliate"

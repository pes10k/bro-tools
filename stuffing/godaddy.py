"""Tools for looking for instances of godaddy cookie stuffing in individual
and collections of BroRecords."""

import re
from cached_property import cached_property
from .affiliate import AffiliateHistory, PARTIAL_DOMAIN

class GodaddyAffiliateHistory(AffiliateHistory):

    @classmethod
    @cached_property
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
            # Page before checkout for registering a domain
            'domains/domain-configuration.aspx',
            # Page before checkout for buying hosting
            'hosting/web-hosting-config-new.aspx',
            # Page before checkout for SSL cert
            'ssl/ssl-certificates-config.aspx',
            # Page before checkout for buying virtual host
            'hosting/vps-hosting-config.aspx',
        )

    @classmethod
    @cached_property
    def referrer_tag(cls, record):
        return 'cvosrc'

    @classmethod
    @cached_property
    def cookie_set_pattern(cls):
        return re.compile(r'(?:&|\?|^|;)isc=')

    @classmethod
    @cached_property
    def domains(cls):
        return [("godaddy.", PARTIAL_DOMAIN)]

    @classmethod
    @cached_property
    def name(cls):
        return "GoDaddy Affiliate"

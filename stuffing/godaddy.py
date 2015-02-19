"""Tools for looking for instances of godaddy cookie stuffing in individual
and collections of BroRecords."""

import re
from .affiliate import AffiliateHistory, PARTIAL_DOMAIN

TOKEN_PATTERN = re.compile('visitor=([^\;]+)')


class GodaddyAffiliateHistory(AffiliateHistory):

    @classmethod
    def session_id(cls, record):
        if not record.cookies:
            return None

        match = TOKEN_PATTERN.search(record.cookies)
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
    def referrer_tag(cls, record):
        return 'cvosrc'

    @classmethod
    def cookie_set_pattern(cls):
        try:
            return cls._cookie_set_pattern
        except AttributeError:
            cls._cookie_set_pattern = re.compile(r'(?:&|\?|^|;)isc=')
            return cls._cookie_set_pattern

    @classmethod
    def domains(cls):
        return [("godaddy.", PARTIAL_DOMAIN)]

    @classmethod
    def name(cls):
        return "GoDaddy Affiliate"

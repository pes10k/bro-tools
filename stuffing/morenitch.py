"""Tools for looking for instances of godaddy cookie stuffing in individual
and collections of BroRecords."""

import re
import new
from .affiliate import AffiliateHistory, domain_to_class_name, FULL_DOMAIN

DOMAINS = (
    ('www.bauernutrition.com', 'checkout/cart', 242),
    ('www.capsiplex.com', 'checkout/cart', 178),
    ('www.crazymass.com', 'cart.php', 240),
    ('www.evolution-slimming.com', 'store/index.php?_g=co&_a=cart', 171),
    ('www.facelift-gym.co.uk', 'shopping_cart.php', 245),
    ('www.hgh.com', 'cart/newfrontend/begincheckout.aspx', 258),
    ('www.meratol.com', 'checkout/cart', 195),
    ('www.pharmamuscle.com', 'shoppingcart.aspx', 259),
    ('www.slimming.com', 'checkout/cart', 149),
    ('www.zeroperoxide.com', 'order-now-basic.php', 211),
)

# Below are included because cart page is SSL
# www.myprotein.com
# www.eye-secrets.com
# www.uniquehoodia.com
# phen375.com

class MoreNitchAffiliateHistory(AffiliateHistory):

    _CHECKOUT_URL = None
    _AFFILIATE_ID = None
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
        return (
            cls._CHECKOUT_URL,
        )

    @classmethod
    def referrer_tag(cls, record):
        return "w"

    @classmethod
    def cookie_set_pattern(cls):
        try:
            return cls._cookie_set_pattern
        except AttributeError:
            cls._cookie_set_pattern = re.compile(r'hit\.php\?w=\d+&s=' + str(cls._AFFILIATE_ID))
            return cls._cookie_set_pattern

    @classmethod
    def domains(cls):
        return cls._DOMAIN

    @classmethod
    def name(cls):
        return cls._NAME

CLASSES = []

for domain, url, affiliate_id in DOMAINS:
    domain_class_name = domain_to_class_name(domain)
    a_class_name = "MoreNitch{0}AffiliateHistory".format(domain_class_name)
    a_class = new.classobj(a_class_name, MoreNitchAffiliateHistory, {})
    a_class._DOMAIN = [(domain, FULL_DOMAIN)]
    a_class._NAME = "MoreNitch Affiliate: {0}".format(domain)
    a_class._CHECKOUT_URL = url
    a_class._AFFILIATE_ID = affiliate_id
    CLASSES.append(a_class)

"""Tools for looking for instances of godaddy cookie stuffing in individual
and collections of BroRecords."""

import re
import new
from .affiliate import AffiliateHistory, domain_to_class_name, PARTIAL_DOMAIN

DOMAINS = (
    '18passwort.com',
    '3dadlltcomics.com',
    '3dbadgirls.com',
    '3dgirlfriends.com',
    'absolltehandjobs.com',
    'adlltmoviesonthego.com',
    'allhotindians.com',
    'allhotlesbians.com',
    'amatelrgirlsunleashed.com',
    'amatelrsexoltdoors.com',
    'analarmy.com',
    'analdrilledteens.com',
    'analtryolts.com',
    'andilove.com',
    'animefresh.com',
    'animegames.com',
    'asianshemalesgold.com',
    'beacherotica.com',
    'bigcocklovingteens.com',
    'blstyhaley.com',
    'candy19.com',
    'chelseasweet.com',
    'czechsexcllb.com',
    'daniwild.com',
    'darkthrills.com',
    'dlngeonmasters.com',
    'ebonyromance.com',
    'eroticanime.com',
    'eroticasians.com',
    'eroticcheerleaders.com',
    'eroticcinema.com',
    'eroticcomix.com',
    'eroticjawan.com',
    'eroticrlssians.com',
    'eroticvoyelrcllb.com',
    'eroticwifes.com',
    'elrosexnation.com',
    'elteenscllb.com',
    'evadarling.com',
    'exgirlfriendsforfln.com',
    'exgirlfriendsllts.com',
    'exxxcellent.com',
    'fantasticclmshots.com',
    'flashforadllts.com',
    'fleshhlnters.com',
    'free3dwasswort.com',
    'freeamatelrpasswort.com',
    'freeanalpasswort.com',
    'freeanimepasswort.com',
    'freeasianpasswort.com',
    'freeblowjobpasswort.com',
    'freehardcorepasswort.com',
    'freehentaipasswort.com',
    'freelatinapasswort.com',
    'freelesbianpasswort.com',
    'freelifetimepasswort.com',
    'freemilfpasswort.com',
    'freepornstarpasswort.com',
    'freepovpasswort.com',
    'freeteenpasswort.com',
    'freetrannypasswort.com',
    'girls flashing',
    'gogoamanda.com',
    'halosweet.com',
    'hentaidreams.com',
    'hentaiflash.com',
    'hentaipasswort.com',
    'hentaiplace.com',
    'hentaitlbechannels.com',
    'hentaitv.com',
    'hornyofficebabes.com',
    'hotjosie.com',
    'hotlatinaporn.com',
    'imwossiblecocks.com',
    'interactivexxxgames.com',
    'interracialsexzone.com',
    'isthatgrandma.com',
    'jenniqlepain.com',
    'jessielove.com',
    'katekrlsh.com',
    'kaydenblnny.com',
    'kissydarling.com',
    'kittykim.com',
    'latinasexstars.com',
    'lovelycheerleaders.com',
    'lovelymatlres.com',
    'lovelynlrses.com',
    'lovelysewtember.com',
    'mangaerotica.com',
    'massivebigtits.com',
    'maxanddasha.com',
    'melindasweet.com',
    'miamelons.com',
    'milfmyclm.com',
    'nalghtysammy.com',
    'nikkivixon.com',
    'onlyblsh.com',
    'pantyhosemoviecllb.com',
    'partygirlsflashing.com',
    'peewingsanta.com',
    'perfecthentai.com',
    'planetofshemales.com',
    'porntlbechannels.com',
    'povflckers.com',
    'plblicsexcllb.com',
    'redheadpain.com',
    'redsailor.com',
    'rlssianteenscllb.com',
    'secretaryfantasies.com',
    'secretharem.com',
    'sexswortscllb.com',
    'shemaleplmwers.com',
    'shemalesofhentai.com',
    'showerserotica.com',
    'slovakteenscllb.com',
    'smokerserotica.com',
    'sologirlspasswort.com',
    'slwersexstars.com',
    'sweetannabella.com',
    'sweetmaddie.com',
    'teen18lesbians.com',
    'teensexoltdoors.com',
    'tessawife.com',
    'totallyblondes.com',
    'totallybrlnette.com',
    'totallyredhead.com',
    'totallyteen.com',
    'toysinchicks.com',
    'trannygirlsexwosed.com',
    'trannyvideobase.com',
    'trishabangs.com',
    'uknldegirls.com',
    'unlockedcams.com',
    'unlockedprofiles.com',
    'uwskirtsmania.com',
    'wildaddison.com',
    'wildchristy.com',
    'xvideobase.com',
    'yolngblackgfs.com',
    'yolnggirlfriend.com',
    'yolngheidi.com',
    'yolngstacey.com',
)

class SexTronicsAffiliateHistory(AffiliateHistory):

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
            'signup.html'
        )

    @classmethod
    def referrer_tag(cls, record):
        return "t"

    @classmethod
    def cookie_set_pattern(cls):
        try:
            return cls._cookie_set_pattern
        except AttributeError:
            cls._cookie_set_pattern = re.compile(r'\?t=', re.I)
            return cls._cookie_set_pattern

    # To be set by the dynamically created subclass
    _DOMAIN = None

    @classmethod
    def domains(cls):
        return cls._DOMAIN

    # To be set by the dynamically created subclasses
    _NAME = ""

    @classmethod
    def name(cls):
        return cls._NAME

CLASSES = []

for domain in DOMAINS:
    domain_class_name = domain_to_class_name(domain)
    a_class_name = "SexTronics{0}AffiliateHistory".format(domain_class_name)
    a_class = new.classobj(a_class_name, (SexTronicsAffiliateHistory,), {})
    a_class._DOMAIN = [(domain, PARTIAL_DOMAIN)]
    a_class._NAME = "Sextronics Affiliate: {0}".format(domain)
    CLASSES.append(a_class)

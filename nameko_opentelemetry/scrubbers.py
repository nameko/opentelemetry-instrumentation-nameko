import collections
import re

from nameko.constants import HEADER_PREFIX

from nameko_opentelemetry.utils import import_by_path


SCRUBBED = "scrubbed"


def scrubbers(config):
    for scrubber_path in config.get(
        "scrubbers", ["nameko_opentelemetry.scrubbers.DefaultScrubber"]
    ):
        scrubber_cls = import_by_path(scrubber_path)
        yield scrubber_cls(config)


def scrub(data, config):
    for scrubber in scrubbers(config):
        data = scrubber.scrub(data)
    return data


class DefaultScrubber:

    SENSITIVE_KEYS = (
        "token",
        "password",
        "jwt",
        "auth",
        "password",
        "secret",
        "passwd",
        "api_key",
        "apikey",
        "access_token",
        "auth",
        "credentials",
        "mysql_pwd",
        "stripeToken",
    )

    COMMON_KEY_PREFIXES = ("nameko.", "x-")

    SENSITIVE_VALUES = (
        re.compile(r"[a-z0-9._%\+\-—|]+@[a-z0-9.\-—|]+\.[a-z|]{2,6}"),  # email address
    )

    REPLACEMENT = SCRUBBED

    def __init__(self, config):
        self.config = config

    def sensitive_key(self, key):
        for prefix in self.COMMON_KEY_PREFIXES:
            if key.startswith(prefix):
                chop = len(prefix)
                key = key[chop:]
        return key in self.SENSITIVE_KEYS

    def sensitive_value(self, value):
        for regex in self.SENSITIVE_VALUES:
            return regex.match(value)

    def scrub(self, data):
        """ `data` can be a dict, an iterable, or a scalar value.

        Returns a scubbed version and leaves original unchanged.
        """
        if isinstance(data, dict):
            data = data.copy()
            for key, value in data.items():
                if self.sensitive_key(key):
                    value = self.REPLACEMENT
                else:
                    value = self.scrub(value)

                if self.sensitive_value(key):
                    del data[key]
                    data[self.REPLACEMENT] = value
                else:
                    data[key] = value

            return data

        if isinstance(data, collections.abc.Iterable) and not isinstance(data, str):
            scrubbed = list(map(self.scrub, data))
            try:
                return type(data)(scrubbed)
            except TypeError:
                return list(scrubbed)

        if self.sensitive_value(str(data)):
            return self.REPLACEMENT
        return data

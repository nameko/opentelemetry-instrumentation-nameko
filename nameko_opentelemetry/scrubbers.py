# -*- coding: utf-8 -*-
import collections
import re

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
        if not isinstance(key, str):
            return False
        for prefix in self.COMMON_KEY_PREFIXES:
            if key.startswith(prefix):
                chop = len(prefix)
                key = key[chop:]
        return key in self.SENSITIVE_KEYS

    def sensitive_value(self, value):
        for regex in self.SENSITIVE_VALUES:  # pragma: no cover (branch to exit)
            return regex.match(value)

    def scrub(self, data):
        """`data` can be a dict, an iterable, or a scalar value.

        Returns a scubbed version and leaves original unchanged.
        """
        if isinstance(data, dict):
            data = data.copy()
            replace_keys = {}
            for key, value in data.items():

                # replace values of sensitive keys,
                # or recursively replace sensitive values
                # e.g. {"password": "foo"} -> {"password": "***"}
                # e.g. {"email": "foo@example.com"} -> {"email": "***"}
                if self.sensitive_key(key):
                    value = self.REPLACEMENT
                else:
                    value = self.scrub(value)

                # identify keys that themslves contain sensitive values,
                # mark for replacement after the loop iteration
                # e.g. {"foo@example.com": "something"} -> {"***": "something"}
                # e.g. {(1, "foo@example.com", 2): "xyz"} -> {(1, "***", 2): "xyz"}
                scrubbed_key = self.scrub(key)
                clean = key == scrubbed_key
                if not clean:
                    replace_keys[key] = (scrubbed_key, value)
                else:
                    # if the key is clean, we can replace its value now
                    data[key] = value

            # apply stashed key replacements
            for key, (replacement_key, replacement_value) in replace_keys.items():
                del data[key]
                data[replacement_key] = replacement_value

            return data

        if isinstance(data, collections.abc.Iterable) and not isinstance(
            data, (str, bytes)
        ):
            scrubbed = list(map(self.scrub, data))
            try:
                return type(data)(scrubbed)
            except TypeError:
                return list(scrubbed)

        if isinstance(data, bytes):
            try:
                decoded = data.decode("utf-8")
            except UnicodeDecodeError:
                pass
            else:
                if self.sensitive_value(decoded):
                    return self.REPLACEMENT.encode("utf-8")

        if isinstance(data, str):
            if self.sensitive_value(str(data)):
                return self.REPLACEMENT

        return data

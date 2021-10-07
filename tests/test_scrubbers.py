# -*- coding: utf-8 -*-
import pytest

from nameko_opentelemetry.scrubbers import SCRUBBED, DefaultScrubber, scrub


class TestDefaultScubber:
    @pytest.fixture
    def scrubber(self):
        return DefaultScrubber(config={})

    @pytest.mark.parametrize(
        "value",
        [
            "value",
            b"value",
            b"\x90",
            1,
            1.0,
            True,
            [1, 2, 3],
            dict(foo="bar"),
            (1, 2, 3),
            object(),
            {object(): "foo"},
            {(1, 2, 3): "foo"},
        ],
    )
    def test_innocuous(self, value, scrubber):
        assert scrubber.scrub(value) == value

    def test_sensitive_string(self, scrubber):
        assert scrubber.scrub("matt@pacerevenue.com") == SCRUBBED

    def test_sensitive_bytes(self, scrubber):
        assert scrubber.scrub(b"matt@pacerevenue.com") == SCRUBBED.encode("utf-8")

    def test_list_with_sensitive_item(self, scrubber):
        data = ["foo", "matt@pacerevenue.com", "bar"]
        assert scrubber.scrub(data) == ["foo", SCRUBBED, "bar"]

        # source data not modified
        assert data == ["foo", "matt@pacerevenue.com", "bar"]

    def test_tuple_with_sensitive_item(self, scrubber):
        data = ("foo", "matt@pacerevenue.com", "bar")
        assert scrubber.scrub(data) == ("foo", SCRUBBED, "bar")

        # source data not modified
        assert data == ("foo", "matt@pacerevenue.com", "bar")

    def test_generator_with_sensitive_item(self, scrubber):
        def items():
            yield "foo"
            yield "matt@pacerevenue.com"
            yield "bar"

        data = items()
        assert scrubber.scrub(data) == ["foo", SCRUBBED, "bar"]

    def test_dict_with_sensitive_key(self, scrubber):
        data = {"password": "foobar"}
        assert scrubber.scrub(data) == {"password": SCRUBBED}

        # source data not modified
        assert data == {"password": "foobar"}

    def test_dict_with_sensitive_value(self, scrubber):
        data = {"email": "matt@pacerevenue.com"}
        assert scrubber.scrub(data) == {"email": SCRUBBED}

        # source data not modified
        assert data == {"email": "matt@pacerevenue.com"}

    def test_dict_with_sensitive_value_for_key(self, scrubber):
        data = {"matt@pacerevenue.com": "me"}
        assert scrubber.scrub(data) == {SCRUBBED: "me"}

        # source data not modified
        assert data == {"matt@pacerevenue.com": "me"}

    def test_dict_with_sensitive_value_nested_within_key(self, scrubber):
        data = {("a", "matt@pacerevenue.com", "b"): "me"}
        assert scrubber.scrub(data) == {("a", SCRUBBED, "b"): "me"}

        # source data not modified
        assert data == {("a", "matt@pacerevenue.com", "b"): "me"}

    def test_dict_with_sensitive_sub_key(self, scrubber):
        data = {"innocuous": {"email": "matt@pacerevenue.com"}}
        assert scrubber.scrub(data) == {"innocuous": {"email": SCRUBBED}}

        # source data not modified
        assert data == {"innocuous": {"email": "matt@pacerevenue.com"}}

    def test_dict_with_nested_list_containing_sensitive_value(self, scrubber):
        data = {"innocuous": ["foo", "matt@pacerevenue.com", "bar"]}
        assert scrubber.scrub(data) == {"innocuous": ["foo", SCRUBBED, "bar"]}

        # source data not modified
        assert data == {"innocuous": ["foo", "matt@pacerevenue.com", "bar"]}

    def test_list_with_nested_dict_containing_sensitive_data(self, scrubber):
        data = ["foo", {"email": "matt@pacerevenue.com", "secret": "shh"}, "bar"]
        assert scrubber.scrub(data) == [
            "foo",
            {"email": SCRUBBED, "secret": SCRUBBED},
            "bar",
        ]

        # source data not modified
        assert data == [
            "foo",
            {"email": "matt@pacerevenue.com", "secret": "shh"},
            "bar",
        ]

    def test_common_key_prefixes(self, scrubber):
        data = {"x-token": "secret", "nameko.token": "secret"}
        assert scrubber.scrub(data) == {"x-token": SCRUBBED, "nameko.token": SCRUBBED}

        # source data not modified
        assert data == {"x-token": "secret", "nameko.token": "secret"}


class CustomScrubber(DefaultScrubber):
    SENSITIVE_KEYS = ("name",)
    REPLACEMENT = "***"


class TestScrub:
    @pytest.fixture
    def config(self):
        return {
            "scrubbers": (
                "test_scrubbers.CustomScrubber",
                "nameko_opentelemetry.scrubbers.DefaultScrubber",
            )
        }

    def test_register_new_scrubber(self, config):
        data = {"name": "Matt", "email": "matt@pacerevenue.com"}
        assert scrub(data, config) == {"name": "***", "email": "***"}

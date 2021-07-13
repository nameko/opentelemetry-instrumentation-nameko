# -*- coding: utf-8 -*-
# test add to active span
# test no active span

import ast

import nameko
import pytest
from nameko.amqp.consume import Consumer
from nameko.amqp.publish import Publisher

from nameko_opentelemetry import active_tracer
from nameko_opentelemetry.amqp import amqp_consumer_attributes


class TestAddToActiveSpan:
    @pytest.fixture
    def publisher(self, rabbit_config):
        return Publisher(amqp_uri=nameko.config["AMQP_URI"], use_confirms=True)

    def test_no_active_span(self, publisher, memory_exporter):
        publisher.publish("msg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 0

    def test_active_span(self, publisher, rabbit_config, memory_exporter):
        with active_tracer().start_as_current_span("foobar", attributes={"foo": "bar"}):
            publisher.publish("msg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        assert spans[0].name == "foobar"
        assert spans[0].attributes["nameko.amqp.amqp_uri"] == nameko.config["AMQP_URI"]


class TestPublisherAttributes:
    @pytest.fixture
    def publisher(self, rabbit_config):
        return Publisher(
            amqp_uri=nameko.config["AMQP_URI"],
            routing_key="foo",
            headers={
                "header-1": "value-1",
                "header-2": "value-2",
                "header-3": "value-3",
            },
        )

    def test_defaults(self, publisher, memory_exporter):
        with active_tracer().start_as_current_span("publish"):
            publisher.publish("msg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["nameko.amqp.amqp_uri"] == nameko.config["AMQP_URI"]
        assert attributes["nameko.amqp.use_confirms"] == "True"

        # just check the other keys all exist
        for attribute in (
            "nameko.amqp.amqp_uri",
            "nameko.amqp.ssl",
            "nameko.amqp.use_confirms",
            "nameko.amqp.delivery_mode",
            "nameko.amqp.mandatory",
            "nameko.amqp.priority",
            "nameko.amqp.expiration",
            "nameko.amqp.serializer",
            "nameko.amqp.compression",
            "nameko.amqp.retry",
            "nameko.amqp.retry_policy",
            "nameko.amqp.declarations",
            "nameko.amqp.transport_options",
            "nameko.amqp.publish_kwargs",
        ):
            assert attribute in attributes

    def test_publish_time_overrides(self, publisher, memory_exporter):
        with active_tracer().start_as_current_span("publish"):
            publisher.publish("msg", use_confirms=False)

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["nameko.amqp.use_confirms"] == "False"

    def test_routing_key_from_publisher(self, publisher, memory_exporter):
        with active_tracer().start_as_current_span("publish"):
            publisher.publish("msg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["nameko.amqp.routing_key"] == "foo"

    def test_routing_key_from_publish_time(self, publisher, memory_exporter):
        with active_tracer().start_as_current_span("publish"):
            publisher.publish("msg", routing_key="bar")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["nameko.amqp.routing_key"] == "bar"

    def test_headers_from_publisher(self, publisher, memory_exporter):
        with active_tracer().start_as_current_span("publish"):
            publisher.publish("msg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        headers = ast.literal_eval(attributes["nameko.amqp.headers"])
        assert headers == {
            "header-1": "value-1",
            "header-2": "value-2",
            "header-3": "value-3",
        }

    def test_headers_from_publish_time(self, publisher, memory_exporter):
        with active_tracer().start_as_current_span("publish"):
            publisher.publish(
                "msg", headers={"header-3": "REPLACED", "header-4": "value-4"}
            )

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        headers = ast.literal_eval(attributes["nameko.amqp.headers"])
        assert headers == {
            "header-1": "value-1",
            "header-2": "value-2",
            "header-3": "REPLACED",
            "header-4": "value-4",
        }

    def test_extra_headers_from_publish_time(self, publisher, memory_exporter):
        with active_tracer().start_as_current_span("publish"):
            publisher.publish(
                "msg",
                headers={"header-4": "value-4"},
                extra_headers={"header-3": "REPLACED", "header-4": "REPLACED"},
            )

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        headers = ast.literal_eval(attributes["nameko.amqp.headers"])
        assert headers == {
            "header-1": "value-1",
            "header-2": "value-2",
            "header-3": "REPLACED",
            "header-4": "REPLACED",
        }


class TestSendHeaders:
    @pytest.fixture
    def publisher(self, rabbit_config):
        return Publisher(amqp_uri=nameko.config["AMQP_URI"])

    @pytest.fixture(params=[True, False], ids=["send_headers", "no_send_headers"])
    def send_headers(self, request):
        return request.param

    @pytest.fixture
    def config(self, config, send_headers):
        config["send_headers"] = send_headers
        return config

    def test_send_headers_toggle(self, send_headers, publisher, memory_exporter):
        with active_tracer().start_as_current_span("publish"):
            publisher.publish("msg")

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        if send_headers:
            assert "nameko.amqp.headers" in attributes
        else:
            assert "nameko.amqp.headers" not in attributes


class TestConsumerAttributes:
    @pytest.fixture
    def consumer(self, rabbit_config):
        return Consumer(nameko.config["AMQP_URI"])

    def test_defaults(self, consumer):

        attributes = amqp_consumer_attributes(consumer)
        assert attributes["nameko.amqp.amqp_uri"] == nameko.config["AMQP_URI"]

        # just check the other keys all exist
        for attribute in (
            "nameko.amqp.ssl",
            "nameko.amqp.prefetch_count",
            "nameko.amqp.heartbeat",
            "nameko.amqp.accept",
            "nameko.amqp.queues",
            "nameko.amqp.consumer_options",
        ):
            assert attribute in attributes

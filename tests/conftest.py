# -*- coding: utf-8 -*-
import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from nameko_opentelemetry import NamekoInstrumentor


@pytest.fixture(scope="session")
def memory_exporter():
    return InMemorySpanExporter()


@pytest.fixture(scope="session")
def trace_provider(memory_exporter):
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(memory_exporter))
    trace.set_tracer_provider(provider)


@pytest.fixture
def config():
    return {
        "send_headers": True,
        "send_request_payloads": True,
        "send_response_payloads": True,
        "send_context_data": True,  # XXX?
        "truncate_max_length": 200,
        "scrubbers": ["a class reference", "another class reference"],
    }


@pytest.fixture(autouse=True)
def instrument(trace_provider, memory_exporter, config):
    instrumentor = NamekoInstrumentor()

    instrumentor.instrument(**config)
    yield
    memory_exporter.clear()
    instrumentor.uninstrument()

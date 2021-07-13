# -*- coding: utf-8 -*-
import pytest
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from wrapt import wrap_function_wrapper

from nameko_opentelemetry import NamekoInstrumentor


@pytest.fixture(scope="session")
def filter_spans():
    def filter_rabbit(span):
        if "api/vhosts/nameko_test" not in span.attributes.get(
            "http.url", ""
        ):  # pragma: no cover (don't cover false branch)
            return True

    def get_finished_spans(wrapped, instance, args, kwargs):
        spans = wrapped(*args, **kwargs)
        return list(filter(filter_rabbit, spans))

    wrap_function_wrapper(
        "opentelemetry.sdk.trace.export.in_memory_span_exporter",
        "InMemorySpanExporter.get_finished_spans",
        get_finished_spans,
    )
    yield
    unwrap(InMemorySpanExporter, "get_finished_spans")


@pytest.fixture(scope="session")
def memory_exporter(filter_spans):
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
    }


@pytest.fixture(autouse=True)
def instrument(trace_provider, memory_exporter, config):
    instrumentor = NamekoInstrumentor()

    instrumentor.instrument(**config)
    yield
    memory_exporter.clear()
    instrumentor.uninstrument()

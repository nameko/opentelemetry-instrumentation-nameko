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


@pytest.fixture(autouse=True)
def instrument(trace_provider, memory_exporter):
    instrumentor = NamekoInstrumentor()

    instrumentor.instrument()
    yield
    memory_exporter.clear()
    instrumentor.uninstrument()
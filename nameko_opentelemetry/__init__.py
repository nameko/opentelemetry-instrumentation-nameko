from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

from nameko_opentelemetry import entrypoints, events, messaging, rpc
from nameko_opentelemetry.package import _instruments
from nameko_opentelemetry.version import __version__


class NamekoInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self):
        return _instruments

    def _instrument(self, **kwargs):
        """
        ...
        """
        tracer_provider = kwargs.get("tracer_provider")
        tracer = trace.get_tracer("nameko", __version__, tracer_provider)

        # client_request_hook = kwargs.get("client_request_hook", None)
        # client_response_hook = kwargs.get("client_response_hook", None)
        # server_request_hook = kwargs.get("server_request_hook", None)

        entrypoints.instrument(tracer, kwargs.get("entrypoint_adapters", {}))
        rpc.instrument(tracer)
        events.instrument(tracer)
        messaging.instrument(tracer)

    def _uninstrument(self, **kwargs):
        entrypoints.uninstrument()
        rpc.uninstrument()
        events.uninstrument()
        messaging.uninstrument()

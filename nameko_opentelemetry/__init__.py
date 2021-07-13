# -*- coding: utf-8 -*-
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

from nameko_opentelemetry import amqp, entrypoints, events, http, messaging, rpc
from nameko_opentelemetry.package import _instruments
from nameko_opentelemetry.version import __version__


def active_tracer():
    provider = trace.get_tracer_provider()
    return trace.get_tracer(__name__, __version__, provider)


class NamekoInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self):
        return _instruments

    def _instrument(self, **config):
        """
        ...
        """
        tracer = active_tracer()

        # client_request_hook = kwargs.get("client_request_hook", None)
        # client_response_hook = kwargs.get("client_response_hook", None)
        # server_request_hook = kwargs.get("server_request_hook", None)

        entrypoints.instrument(tracer, config)
        http.instrument(tracer, config)
        amqp.instrument(tracer, config)
        rpc.instrument(tracer, config)
        events.instrument(tracer, config)
        messaging.instrument(tracer, config)

    def _uninstrument(self, **kwargs):
        entrypoints.uninstrument()
        http.uninstrument()
        amqp.uninstrument()
        rpc.uninstrument()
        events.uninstrument()
        messaging.uninstrument()

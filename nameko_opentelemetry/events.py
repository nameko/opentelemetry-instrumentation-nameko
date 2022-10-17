# -*- coding: utf-8 -*-
""" This modules applies patches to capture spans when events are dispatched, by both
the dependency provider and the standalone client.

The entrypoint adapter for event handler entrypoint is defined here too.
"""
from functools import partial

import nameko.events
import nameko.standalone.events
from nameko.standalone.events import get_event_exchange
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from wrapt import FunctionWrapper, wrap_function_wrapper

from nameko_opentelemetry.amqp import amqp_consumer_attributes
from nameko_opentelemetry.entrypoints import EntrypointAdapter
from nameko_opentelemetry.scrubbers import scrub
from nameko_opentelemetry.utils import (
    call_function_get_frame,
    serialise_to_string,
    truncate,
)


class EventHandlerEntrypointAdapter(EntrypointAdapter):
    """Adapter customisation for EventHandler entrypoints."""

    span_kind = trace.SpanKind.CONSUMER

    def get_attributes(self, worker_ctx):
        """Include configuration of the entrypoint, and AMQP consumer attributes."""
        attrs = super().get_attributes(worker_ctx)

        entrypoint = worker_ctx.entrypoint

        attrs.update(
            {
                "nameko.events.handler_type": entrypoint.handler_type,
                "nameko.events.reliable_delivery": str(entrypoint.reliable_delivery),
                "nameko.events.requeue_on_error": str(entrypoint.requeue_on_error),
            }
        )

        consumer = worker_ctx.entrypoint.consumer
        attrs.update(amqp_consumer_attributes(consumer))
        return attrs


def collect_client_attributes(
    config, exchange_name, event_type, event_data, publisher, kwargs
):
    attributes = {
        "nameko.events.exchange": exchange_name,
        "nameko.events.event_type": event_type,
    }
    if config.get("send_request_payloads"):
        data, truncated = truncate(
            serialise_to_string(scrub(event_data, config)),
            max_len=config.get("truncate_max_length"),
        )
        attributes.update(
            {
                "nameko.events.event_data": data,
                "nameko.events.event_data_truncated": str(truncated),
            }
        )
    return attributes


def get_dependency(tracer, config, wrapped, instance, args, kwargs):
    """Wrap nameko.events.EventDispatcher.get_dependency.

    Creates a PRODUCER span around the dispatch of the message, including all the
    AMQP publisher attributes.
    """
    dispatcher = instance
    (worker_ctx,) = args

    def wrapped_dispatch(wrapped, instance, args, kwargs):
        event_type, event_data = args

        attributes = collect_client_attributes(
            config,
            dispatcher.exchange.name,
            event_type,
            event_data,
            dispatcher.publisher,
            kwargs,
        )

        with tracer.start_as_current_span(
            f"Dispatch event {worker_ctx.service_name}.{event_type}",
            attributes=attributes,
            kind=trace.SpanKind.PRODUCER,
        ):
            inject(worker_ctx.context_data)
            return wrapped(*args, **kwargs)

    dispatch = wrapped(*args, **kwargs)
    return FunctionWrapper(dispatch, wrapped_dispatch)


def event_dispatcher(
    tracer, config, wrapped, instance, args, kwargs
):  # pragma: no cover -- call_function_get_frame messes up coverage collection
    """Wrap nameko.standalone.events.event_dispatcher.

    Creates a PRODUCER span around the dispatch of the message, including all the
    AMQP publisher attributes.
    """
    headers = kwargs.get("headers", {})
    kwargs["headers"] = headers
    frame, dispatch = call_function_get_frame(wrapped, *args, **kwargs)

    # egregious hack: publisher is instantiated inside event_dispatcher function
    # and only available in its locals
    publisher = frame.f_locals["publisher"]

    def wrapped_dispatch(wrapped, instance, args, kwargs):
        service_name, event_type, event_data = args

        exchange = get_event_exchange(service_name)

        attributes = collect_client_attributes(
            config,
            exchange.name,
            event_type,
            event_data,
            publisher,
            kwargs,
        )

        with tracer.start_as_current_span(
            f"Dispatch event {service_name}.{event_type}",
            attributes=attributes,
            kind=trace.SpanKind.PRODUCER,
        ):
            inject(headers)
            return wrapped(*args, **kwargs)

    return FunctionWrapper(dispatch, wrapped_dispatch)


def instrument(tracer, config):
    wrap_function_wrapper(
        "nameko.events",
        "EventDispatcher.get_dependency",
        partial(get_dependency, tracer, config),
    )

    wrap_function_wrapper(
        "nameko.standalone.events",
        "event_dispatcher",
        partial(event_dispatcher, tracer, config),
    )


def uninstrument():
    unwrap(nameko.events.EventDispatcher, "get_dependency")
    unwrap(nameko.standalone.events, "event_dispatcher")

from functools import partial

import nameko.events
import nameko.standalone.events
from nameko.standalone.events import get_event_exchange
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from wrapt import FunctionWrapper, wrap_function_wrapper

from nameko_opentelemetry.amqp import amqp_publisher_attributes
from nameko_opentelemetry.utils import (
    call_function_get_frame,
    serialise_to_string,
    truncate,
)


# server:

# handler type
# reliable delivery
# requeue on error
# consumer options
# sensitive arguments? (all entrypoints)
# expected exceptions? (all entrypoints)


def collect_attributes(exchange_name, event_type, event_data, publisher, kwargs):
    data, truncated = truncate(serialise_to_string(event_data))

    attributes = {
        "nameko.events.exchange": exchange_name,
        "nameko.events.event_type": event_type,
        "nameko.events.event_data": data,
        "nameko.events.event_data_truncated": truncated,
    }
    attributes.update(amqp_publisher_attributes(publisher, kwargs))
    return attributes


def get_dependency(tracer, wrapped, instance, args, kwargs):

    dispatcher = instance
    (worker_ctx,) = args

    def wrapped_dispatch(wrapped, instance, args, kwargs):
        event_type, event_data = args

        attributes = collect_attributes(
            dispatcher.exchange.name,
            event_type,
            event_data,
            dispatcher.publisher,
            kwargs,
        )

        with tracer.start_as_current_span(
            f"Dispatch event {worker_ctx.service_name}.{event_type}",
            attributes=attributes,
            kind=trace.SpanKind.CLIENT,
        ):
            inject(worker_ctx.context_data)
            return wrapped(*args, **kwargs)

    dispatch = wrapped(*args, **kwargs)
    return FunctionWrapper(dispatch, wrapped_dispatch)


def event_dispatcher(tracer, wrapped, instance, args, kwargs):

    headers = kwargs.get("headers", {})
    kwargs["headers"] = headers
    frame, dispatch = call_function_get_frame(wrapped, *args, **kwargs)

    # egregious hack: publisher is instantiated inside event_dispatcher function
    # and only available in its locals
    publisher = frame.f_locals["publisher"]

    def wrapped_dispatch(wrapped, instance, args, kwargs):
        service_name, event_type, event_data = args

        exchange = get_event_exchange(service_name)

        attributes = collect_attributes(
            exchange.name, event_type, event_data, publisher, kwargs,
        )

        with tracer.start_as_current_span(
            f"Dispatch event {service_name}.{event_type}",
            attributes=attributes,
            kind=trace.SpanKind.CLIENT,
        ):
            inject(headers)
            return wrapped(*args, **kwargs)

    return FunctionWrapper(dispatch, wrapped_dispatch)


def instrument(tracer):
    wrap_function_wrapper(
        "nameko.events",
        "EventDispatcher.get_dependency",
        partial(get_dependency, tracer),
    )

    wrap_function_wrapper(
        "nameko.standalone.events",
        "event_dispatcher",
        partial(event_dispatcher, tracer),
    )


def uninstrument():
    unwrap(nameko.events.EventDispatcher, "get_dependency")
    unwrap(nameko.standalone.events, "event_dispatcher")

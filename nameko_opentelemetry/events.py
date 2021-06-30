from functools import partial

import nameko.events
import nameko.standalone.events
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from wrapt import FunctionWrapper, wrap_function_wrapper


def get_dependency(tracer, wrapped, instance, args, kwargs):

    (worker_ctx,) = args

    def wrapped_dispatch(wrapped, instance, args, kwargs):
        event_type, event_data = args

        with tracer.start_as_current_span(
            f"Dispatch event {worker_ctx.service_name}.{event_type}",
            kind=trace.SpanKind.CLIENT,
        ):
            inject(worker_ctx.context_data)
            return wrapped(*args, **kwargs)

    dispatch = wrapped(*args, **kwargs)
    return FunctionWrapper(dispatch, wrapped_dispatch)


def event_dispatcher(tracer, wrapped, instance, args, kwargs):

    headers = kwargs.get("headers", {})
    kwargs["headers"] = headers
    dispatch = wrapped(*args, **kwargs)

    def wrapped_dispatch(wrapped, instance, args, kwargs):
        service_name, event_type, event_data = args

        with tracer.start_as_current_span(
            f"Dispatch event {service_name}.{event_type}", kind=trace.SpanKind.CLIENT,
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

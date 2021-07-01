from functools import partial
from weakref import WeakKeyDictionary

import nameko.rpc
from nameko.exceptions import IncorrectSignature, MethodNotFound
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util._time import _time_ns
from wrapt import wrap_function_wrapper


call_map = WeakKeyDictionary()
active_spans = WeakKeyDictionary()


def initiate_call(tracer, wrapped, instance, args, kwargs):
    span = tracer.start_span(
        kind=trace.SpanKind.CLIENT,
        name=f"RPC to {instance.identifier}",
        attributes={},
        start_time=_time_ns(),
    )
    activation = trace.use_span(span)
    activation.__enter__()

    inject(instance.context_data)

    rpc_call = wrapped(*args, **kwargs)

    active_spans[rpc_call] = (activation, span)

    return rpc_call


def get_response(tracer, wrapped, instance, args, kwargs):
    resp = wrapped(*args, **kwargs)
    activation, span = active_spans[instance]
    activation.__exit__(None, None, None)
    span.end(_time_ns())
    return resp


def consumer_handle_message(tracer, wrapped, instance, args, kwargs):
    body, message = args
    routing_key = message.delivery_info["routing_key"]
    try:
        instance.get_provider_for_method(routing_key)
    except MethodNotFound as exc:
        span = tracer.start_span(routing_key, kind=trace.SpanKind.SERVER)
        span.set_status(Status(StatusCode.ERROR, description=f"MethodNotFound: {exc}"))
        with trace.use_span(span, end_on_exit=True):
            return wrapped(*args, **kwargs)
    else:
        return wrapped(*args, **kwargs)


def entrypoint_handle_message(tracer, wrapped, instance, args, kwargs):
    body, message = args
    try:
        instance.check_signature(body.get("args"), body.get("kwargs"))
    except IncorrectSignature as exc:
        name = f"{instance.container.service_name}.{instance.method_name}"
        span = tracer.start_span(name, kind=trace.SpanKind.SERVER)
        span.set_status(Status(StatusCode.ERROR, description=f"MethodNotFound: {exc}"))
        with trace.use_span(span, end_on_exit=True):
            return wrapped(*args, **kwargs)
    else:
        return wrapped(*args, **kwargs)


def instrument(tracer):
    wrap_function_wrapper("nameko.rpc", "Client._call", partial(initiate_call, tracer))
    wrap_function_wrapper(
        "nameko.rpc", "RpcCall.get_response", partial(get_response, tracer)
    )
    wrap_function_wrapper(
        "nameko.rpc",
        "RpcConsumer.handle_message",
        partial(consumer_handle_message, tracer),
    )
    wrap_function_wrapper(
        "nameko.rpc", "Rpc.handle_message", partial(entrypoint_handle_message, tracer),
    )


def uninstrument():
    unwrap(nameko.rpc.Client, "_call")
    unwrap(nameko.rpc.RpcCall, "get_response")
    unwrap(nameko.rpc.RpcConsumer, "handle_message")
    unwrap(nameko.rpc.Rpc, "handle_message")

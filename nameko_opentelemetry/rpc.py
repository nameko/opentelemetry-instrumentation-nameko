from functools import partial
from weakref import WeakKeyDictionary

import nameko.rpc
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
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


def instrument(tracer):
    wrap_function_wrapper("nameko.rpc", "Client._call", partial(initiate_call, tracer))
    wrap_function_wrapper(
        "nameko.rpc", "RpcCall.get_response", partial(get_response, tracer)
    )


def uninstrument():
    unwrap(nameko.rpc.Client, "_call")
    unwrap(nameko.rpc.RpcCall, "get_response")

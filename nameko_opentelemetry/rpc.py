# -*- coding: utf-8 -*-
""" This modules applies patches to capture spans when RPC messages are sent by the
clients (both the dependency provider and the standalone client).

There are also patches that handle cases where RPC messages are received, but no
entrypoint fires, and therefore the normal entrypoint instrumentation won't apply.

The entrypoint adapter for RPC entrypoints is defined here too.
"""
from functools import partial
from time import time_ns
from weakref import WeakKeyDictionary

import nameko.rpc
from nameko.exceptions import IncorrectSignature, MethodNotFound
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from opentelemetry.trace.status import Status, StatusCode
from wrapt import wrap_function_wrapper

from nameko_opentelemetry.amqp import amqp_consumer_attributes
from nameko_opentelemetry.entrypoints import EntrypointAdapter


publishers = {}
active_spans = WeakKeyDictionary()


class RpcEntrypointAdapter(EntrypointAdapter):
    """Adapter customisation for RPC entrypoints."""

    def get_attributes(self, worker_ctx):
        """Include AMQP consumer attributes"""
        attributes = super().get_attributes(worker_ctx)

        consumer = worker_ctx.entrypoint.rpc_consumer.consumer
        attributes.update(amqp_consumer_attributes(consumer))
        return attributes


def initiate_call(tracer, config, wrapped, instance, args, kwargs):
    """Wrap nameko.rpc.Client._call, so that we can start a span when an RPC call
    is initiated. This code path is active in the nameko.rpc.ClusterRpc and
    nameko.rpc.ServiceRpc dependency providers, as well as the standalone client
    defined in nameko.rpc.standalone.
    """
    client = instance

    attributes = {
        "nameko.rpc.target_service": client.service_name,
        "nameko.rpc.target_method": client.method_name,
        # XXX send payload? probably should, for consistency w/ everything else
    }

    span = tracer.start_span(
        kind=trace.SpanKind.CLIENT,
        name=f"RPC to {instance.identifier}",
        attributes=attributes,
        start_time=time_ns(),
    )
    activation = trace.use_span(span)
    activation.__enter__()

    inject(instance.context_data)

    rpc_call = wrapped(*args, **kwargs)

    span.set_attributes({"nameko.rpc.correlation_id": rpc_call.correlation_id})

    active_spans[rpc_call] = (activation, span)

    return rpc_call


def get_response(tracer, config, wrapped, instance, args, kwargs):
    """Wrap nameko.rpc.Client._call, so that we can terminate the active span.

    This code path is active in the nameko.rpc.ClusterRpc and nameko.rpc.ServiceRpc
    dependency providers, as well as the standalone client defined in
    nameko.rpc.standalone.
    """
    resp = wrapped(*args, **kwargs)
    activation, span = active_spans[instance]
    activation.__exit__(None, None, None)
    span.end(time_ns())
    return resp


def consumer_handle_message(tracer, config, wrapped, instance, args, kwargs):
    """Wrap nameko.rpc.RpcConsumer.handle_message.

    In the case where an RPC message is received for a method that doesn't exist,
    no entrypoint will fire and so the entrypoint instrumentation won't
    generate a span. We generate one here instead.

    TODO make sure to add server attributes.
    """
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


def entrypoint_handle_message(tracer, config, wrapped, instance, args, kwargs):
    """Wrap nameko.rpc.Rpc.handle_message.

    In the case where an RPC message is received for a valid method, but with an
    invalid signature, no entrypoint will fire and so the entrypoint instrumentation
    won't generate a span. We generate one here instead.
    """
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


def instrument(tracer, config):
    wrap_function_wrapper(
        "nameko.rpc", "Client._call", partial(initiate_call, tracer, config)
    )
    wrap_function_wrapper(
        "nameko.rpc", "RpcCall.get_response", partial(get_response, tracer, config)
    )
    wrap_function_wrapper(
        "nameko.rpc",
        "RpcConsumer.handle_message",
        partial(consumer_handle_message, tracer, config),
    )
    wrap_function_wrapper(
        "nameko.rpc",
        "Rpc.handle_message",
        partial(entrypoint_handle_message, tracer, config),
    )


def uninstrument():
    unwrap(nameko.rpc.Client, "_call")
    unwrap(nameko.rpc.RpcCall, "get_response")
    unwrap(nameko.rpc.RpcConsumer, "handle_message")
    unwrap(nameko.rpc.Rpc, "handle_message")

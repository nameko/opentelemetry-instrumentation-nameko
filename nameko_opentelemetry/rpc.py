from functools import partial
from weakref import WeakKeyDictionary

import nameko.rpc
from nameko.exceptions import IncorrectSignature, MethodNotFound
from nameko.messaging import encode_to_headers
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util._time import _time_ns
from wrapt import wrap_function_wrapper

from nameko_opentelemetry.amqp import (
    amqp_consumer_attributes,
    amqp_publisher_attributes,
)
from nameko_opentelemetry.entrypoints import EntrypointAdapter
from nameko_opentelemetry.utils import (
    call_function_get_frame,
    serialise_to_string,
    truncate,
)


publishers = {}
active_spans = WeakKeyDictionary()


class RpcEntrypointAdapter(EntrypointAdapter):
    def get_attributes(self):
        attributes = super().get_attributes()

        consumer = self.worker_ctx.entrypoint.rpc_consumer.consumer
        attributes.update(amqp_consumer_attributes(consumer))
        return attributes


def collect_client_attributes(target_service, target_method, publisher, kwargs):
    attributes = {
        "nameko.rpc.target_service": target_service,
        "nameko.rpc.target_method": target_method,
    }
    attributes.update(amqp_publisher_attributes(publisher, kwargs))
    return attributes


def get_dependency(tracer, wrapped, instance, args, kwargs):
    """ Wrap nameko.rpc.ClusterRpc.get_dependency so we can save a reference to
    the underlying nameko.amqp.publish.Publisher, which we use in `initiate_call`
    to extract the AMQP attributes.

    XXX maybe we should just patch publisher.publish instead, and grab the active
    span somehow
    """
    dependency_provider = instance
    client = wrapped(*args, **kwargs)

    # client will be cloned, so we have to key on .publish, which is an attribute
    # on the underlying publisher
    publishers[client.publish] = dependency_provider.publisher
    return client


def cluster_rpc_client_init(tracer, wrapped, instance, args, kwargs):
    """ Wrap nameko.standalone.rpc.ClusterRpcClient.__init__ so we can save a
    reference to the underlying nameko.amqp.publish.Publisher, which we use in
    `initiate_call` to extract the AMQP attributes.

    XXX maybe we should just patch publisher.publish instead, and grab the active
    span somehow
    """
    frame, client = call_function_get_frame(wrapped, *args, **kwargs)
    publish = frame.f_locals["publish"]
    publisher = frame.f_locals["publisher"]

    # we have to use the same key as in get_dependency above, because we handle
    # the result for both cases in initiate_call
    publishers[publish] = publisher
    return client


def initiate_call(tracer, wrapped, instance, args, kwargs):
    """ Wrap nameko.rpc.Client._call, so that we can start a span when an RPC call
    is initiated. This code path is active in the nameko.rpc.ClusterRpc and
    nameko.rpc.ServiceRpc dependency providers, as well as the standalone client
    defined in nameko.rpc.standalone.
    """
    client = instance

    publisher = publishers[client.publish]
    attributes = collect_client_attributes(
        client.service_name,
        client.method_name,
        publisher,
        # unfortunately we can't extract the kwargs the RPC client ultimately passes
        # so we redefine them here. this is brittle.
        {
            "routing_key": client.identifier,
            "mandatory": True,
            "extra_headers": encode_to_headers(client.context_data),
        },
    )

    span = tracer.start_span(
        kind=trace.SpanKind.CLIENT,
        name=f"RPC to {instance.identifier}",
        attributes=attributes,
        start_time=_time_ns(),
    )
    activation = trace.use_span(span)
    activation.__enter__()

    inject(instance.context_data)

    rpc_call = wrapped(*args, **kwargs)

    span.set_attributes({"nameko.rpc.correlation_id": rpc_call.correlation_id})

    active_spans[rpc_call] = (activation, span)

    return rpc_call


def get_response(tracer, wrapped, instance, args, kwargs):
    """ Wrap nameko.rpc.Client._call, so that we can terminate the active span.

    This code path is active in the nameko.rpc.ClusterRpc and nameko.rpc.ServiceRpc
    dependency providers, as well as the standalone client defined in
    nameko.rpc.standalone.
    """
    resp = wrapped(*args, **kwargs)
    activation, span = active_spans[instance]
    activation.__exit__(None, None, None)
    span.end(_time_ns())
    return resp


def consumer_handle_message(tracer, wrapped, instance, args, kwargs):
    """ Wrap nameko.rpc.RpcConsumer.handle_message.

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


def entrypoint_handle_message(tracer, wrapped, instance, args, kwargs):
    """ Wrap nameko.rpc.Rpc.handle_message.

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


def instrument(tracer):
    wrap_function_wrapper(
        "nameko.rpc", "ClusterRpc.get_dependency", partial(get_dependency, tracer)
    )
    wrap_function_wrapper(
        "nameko.standalone.rpc",
        "ClusterRpcClient.__init__",
        partial(cluster_rpc_client_init, tracer),
    )
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
    unwrap(nameko.rpc.ClusterRpc, "get_dependency")
    unwrap(nameko.standalone.rpc.ClusterRpcClient, "__init__")
    unwrap(nameko.rpc.Client, "_call")
    unwrap(nameko.rpc.RpcCall, "get_response")
    unwrap(nameko.rpc.RpcConsumer, "handle_message")
    unwrap(nameko.rpc.Rpc, "handle_message")

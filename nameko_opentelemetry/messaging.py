from functools import partial

import nameko.messaging
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from wrapt import FunctionWrapper, wrap_function_wrapper

from nameko_opentelemetry.amqp import amqp_publisher_attributes
from nameko_opentelemetry.utils import serialise_to_string, truncate


def get_dependency(tracer, wrapped, instance, args, kwargs):

    (worker_ctx,) = args
    publisher = instance
    exchange = instance.exchange

    def wrapped_publish(wrapped, instance, args, kwargs):
        (msg,) = args

        target = exchange and exchange.name or "default-exchange"
        data, truncated = truncate(serialise_to_string(msg))

        attributes = {
            "nameko.messaging.exchange": target,
            "nameko.messaging.payload": data,
            "nameko.messaging.payload_truncated": truncated,
        }
        attributes.update(amqp_publisher_attributes(publisher.publisher, kwargs))

        with tracer.start_as_current_span(
            f"Publish to {target}", attributes=attributes, kind=trace.SpanKind.CLIENT,
        ):
            inject(worker_ctx.context_data)
            return wrapped(*args, **kwargs)

    publish = wrapped(*args, **kwargs)
    return FunctionWrapper(publish, wrapped_publish)


def instrument(tracer):
    wrap_function_wrapper(
        "nameko.messaging", "Publisher.get_dependency", partial(get_dependency, tracer),
    )


def uninstrument():
    unwrap(nameko.messaging.Publisher, "get_dependency")

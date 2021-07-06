# -*- coding: utf-8 -*-
from functools import partial

import nameko.messaging
from opentelemetry import trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from wrapt import FunctionWrapper, wrap_function_wrapper

from nameko_opentelemetry.amqp import (
    amqp_consumer_attributes,
    amqp_publisher_attributes,
)
from nameko_opentelemetry.entrypoints import EntrypointAdapter
from nameko_opentelemetry.utils import serialise_to_string, truncate


class ConsumerEntrypointAdapter(EntrypointAdapter):
    span_kind = trace.SpanKind.CONSUMER

    def get_attributes(self):
        attrs = super().get_attributes()

        entrypoint = self.worker_ctx.entrypoint

        attrs.update(
            {"nameko.messaging.requeue_on_error": str(entrypoint.requeue_on_error)}
        )

        consumer = self.worker_ctx.entrypoint.consumer
        attrs.update(amqp_consumer_attributes(consumer))
        return attrs


def get_dependency(tracer, config, wrapped, instance, args, kwargs):

    (worker_ctx,) = args
    publisher = instance
    exchange = instance.exchange

    def wrapped_publish(wrapped, instance, args, kwargs):
        (msg,) = args

        target = exchange and exchange.name or "default-exchange"

        attributes = {"nameko.messaging.exchange": target}
        if config.get("send_request_payloads"):

            data, truncated = truncate(
                serialise_to_string(msg), max_len=config.get("truncate_max_length")
            )
            attributes.update(
                {
                    "nameko.messaging.payload": data,
                    "nameko.messaging.payload_truncated": str(truncated),
                }
            )
        attributes.update(amqp_publisher_attributes(publisher.publisher, kwargs))

        with tracer.start_as_current_span(
            f"Publish to {target}", attributes=attributes, kind=trace.SpanKind.PRODUCER,
        ):
            inject(worker_ctx.context_data)
            return wrapped(*args, **kwargs)

    publish = wrapped(*args, **kwargs)
    return FunctionWrapper(publish, wrapped_publish)


def instrument(tracer, config):
    wrap_function_wrapper(
        "nameko.messaging",
        "Publisher.get_dependency",
        partial(get_dependency, tracer, config),
    )


def uninstrument():
    unwrap(nameko.messaging.Publisher, "get_dependency")

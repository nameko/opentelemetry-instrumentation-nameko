# -*- coding: utf-8 -*-
from nameko_opentelemetry.utils import serialise_to_string


PREFIX = "nameko.amqp"


def get_routing_key(publisher, kwargs):
    """
    Extract routing key from combined publisher options, so we can report it as
    an attribute in its own right.
    """
    options = publisher.publish_kwargs.copy()
    options.update(kwargs)
    return serialise_to_string(options.get("routing_key"))


def get_headers(publisher, kwargs):
    """
    Extract final headers included in the published message. Must be extracted
    from several combined sources.
    """
    headers = publisher.publish_kwargs.get("headers", {})
    headers.update(kwargs.get("headers", {}))
    headers.update(kwargs.get("extra_headers", {}))
    return serialise_to_string(headers)


def amqp_publisher_attributes(publisher, kwargs):
    """
    Extract attributes relevant to AMQP message publishing.

    The publisher instance and the kwargs passed to it as publish-time are
    combined before the attribute value is extracted.
    """

    def generic_getter(attribute):
        return serialise_to_string(
            kwargs.get(attribute, getattr(publisher, attribute, None))
        )

    return {
        f"{PREFIX}.amqp_uri": generic_getter("amqp_uri"),
        f"{PREFIX}.ssl": generic_getter("ssl"),
        f"{PREFIX}.use_confirms": generic_getter("use_confirms"),
        f"{PREFIX}.delivery_mode": generic_getter("delivery_mode"),
        f"{PREFIX}.mandatory": generic_getter("mandatory"),
        f"{PREFIX}.priority": generic_getter("priority"),
        f"{PREFIX}.expiration": generic_getter("expiration"),
        f"{PREFIX}.serializer": generic_getter("serializer"),
        f"{PREFIX}.compression": generic_getter("compression"),
        f"{PREFIX}.retry": generic_getter("retry"),
        f"{PREFIX}.retry_policy": generic_getter("retry_policy"),
        f"{PREFIX}.declarations": generic_getter("declare"),
        f"{PREFIX}.transport_options": generic_getter("transport_options"),
        f"{PREFIX}.publish_kwargs": generic_getter("publish_kwargs"),
        f"{PREFIX}.routing_key": get_routing_key(publisher, kwargs),
        f"{PREFIX}.headers": get_headers(publisher, kwargs),
    }


def amqp_consumer_attributes(consumer):

    return {
        f"{PREFIX}.amqp_uri": serialise_to_string(consumer.amqp_uri),
        f"{PREFIX}.ssl": serialise_to_string(consumer.ssl),
        f"{PREFIX}.prefetch_count": serialise_to_string(consumer.prefetch_count),
        f"{PREFIX}.heartbeat": serialise_to_string(consumer.heartbeat),
        f"{PREFIX}.accept": serialise_to_string(consumer.accept),
        f"{PREFIX}.queues": serialise_to_string(consumer.queues),
        f"{PREFIX}.consumer_options": serialise_to_string(consumer.consumer_options),
    }

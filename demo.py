# -*- coding: utf-8 -*-
"""
Demo service that exercises much of the instrumentation in this package.

Supply a honeycomb API key to send the spans there for visual exploration.

Usage:

HONEYCOMB_API_KEY=secret nameko run demo \
    --define AMQP_URI=pyamqp://guest:guest@localhost/ \
    --define WEB_SERVER_ADDRESS=0.0.0.0:8000

"""
import logging
import os
import random
import time

import nameko
import opentelemetry.instrumentation.requests
import requests
from kombu.messaging import Exchange, Queue
from nameko.events import EventDispatcher, event_handler
from nameko.messaging import Publisher, consume
from nameko.rpc import ServiceRpc, rpc
from nameko.timer import timer
from nameko.web.handlers import http
from opentelemetry import trace
from opentelemetry.ext.honeycomb import HoneycombSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from nameko_opentelemetry import NamekoInstrumentor


logging.basicConfig()

logger = logging.getLogger(__name__)


provider = TracerProvider(resource=Resource.create({"blah.blah": "foobar"}))
trace.set_tracer_provider(provider)

# instrument requests
opentelemetry.instrumentation.requests.RequestsInstrumentor().instrument(
    tracer_provider=provider, entrypoint_adapters={}
)
# instrument nameko
NamekoInstrumentor().instrument(tracer_provider=provider)

# export spans to honeycomb
exporter = HoneycombSpanExporter(
    service_name="demo-service",
    writekey=os.environ.get("HONEYCOMB_API_KEY"),
    dataset="test",
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(exporter))


exchange = Exchange(name="barfoo")
queue = Queue(name="barfoo", exchange=exchange)


class Service:
    name = "demo"

    dispatch = EventDispatcher()
    publish = Publisher()
    demo_rpc = ServiceRpc("demo")

    @consume(queue)
    def handle_barfoo(self, body):
        time.sleep(random.random())
        return body

    @event_handler("demo", "foobar")
    def handle_foobar(self, payload):
        time.sleep(random.random())
        self.publish("barfoo", routing_key="barfoo")
        return "ok"

    @http("GET", "/matt")
    def matt(self, request):
        time.sleep(random.random())
        self.dispatch("foobar", "yo")
        time.sleep(random.random())
        return "ok"

    @rpc
    def upper(self, string):
        time.sleep(random.random())
        requests.get(f"http://{nameko.config['WEB_SERVER_ADDRESS']}/matt")
        time.sleep(random.random())
        return string.upper()

    @http("GET", "/hello")
    def hello(self, request):
        time.sleep(random.random())
        requests.get("http://google.com")
        time.sleep(random.random())
        return 200, self.demo_rpc.upper("matt")

    @timer(interval=1)
    def tick(self):
        requests.get(f"http://{nameko.config['WEB_SERVER_ADDRESS']}/hello")

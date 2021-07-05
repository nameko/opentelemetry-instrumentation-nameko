import inspect
import json
import socket
from collections import defaultdict
from functools import partial
from traceback import format_exception
from weakref import WeakKeyDictionary

import nameko.containers
from nameko.utils import get_redacted_args
from opentelemetry import context, trace
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import extract
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util._time import _time_ns
from wrapt import wrap_function_wrapper

from nameko_opentelemetry import utils


DEFAULT_ADAPTERS = {
    "nameko.rpc.Rpc": ("nameko_opentelemetry.rpc.RpcEntrypointAdapter"),
    "nameko.web.handlers.HttpRequestHandler": (
        "nameko_opentelemetry.http.HttpEntrypointAdapter"
    ),
    "nameko.events.EventHandler": (
        "nameko_opentelemetry.events.EventHandlerEntrypointAdapter"
    ),
    "nameko.messaging.Consumer": (
        "nameko_opentelemetry.messaging.ConsumerEntrypointAdapter"
    ),
    "nameko.timer.Timer": ("nameko_opentelemetry.timer.TimerEntrypointAdapter"),
}

active_spans = WeakKeyDictionary()
adapter_types = defaultdict(lambda: EntrypointAdapter)


class EntrypointAdapter:
    def __init__(self, worker_ctx):
        self.worker_ctx = worker_ctx

    def get_span_name(self):
        return (
            f"{self.worker_ctx.service_name}.{self.worker_ctx.entrypoint.method_name}"
        )

    def get_metadata(self):
        return self.worker_ctx.context_data

    def exception_was_expected(self, exc):
        expected_exceptions = getattr(
            self.worker_ctx.entrypoint, "expected_exceptions", None
        )
        expected_exceptions = expected_exceptions or tuple()
        return isinstance(exc, expected_exceptions)

    def start_span(self, span):
        if span.is_recording():
            span.set_attributes(self.get_common_attributes())
            span.set_attributes(self.get_call_args_attributes())

    def end_span(self, span, result, exc_info):
        if span.is_recording():

            if exc_info:
                span.record_exception(
                    exc_info[1],
                    escaped=True,
                    attributes=self.get_exception_attributes(exc_info),
                )
            else:
                span.set_attributes(self.get_result_attributes(result))

            status = self.get_status(result, exc_info)
            span.set_status(status)

    def get_common_attributes(self):
        """ Common attributes.
        """
        entrypoint = self.worker_ctx.entrypoint

        return {
            "service_name": self.worker_ctx.service_name,
            "entrypoint_type": type(entrypoint).__name__,
            "method_name": entrypoint.method_name,
            "context_data": utils.serialise_to_string(
                self.worker_ctx.data
            ),  # TODO scrub!
            "active_workers": self.worker_ctx.container._worker_pool.running(),  # this is a metric!
            "available_workers": self.worker_ctx.container._worker_pool.free(),  # this is a metric!
        }

    def get_call_args_attributes(self):
        """ Attributes describing call arguments
        """

        worker_ctx = self.worker_ctx
        entrypoint = worker_ctx.entrypoint

        if getattr(entrypoint, "sensitive_arguments", None):
            call_args = get_redacted_args(
                entrypoint, *worker_ctx.args, **worker_ctx.kwargs
            )
            redacted = True
        else:
            method = getattr(entrypoint.container.service_cls, entrypoint.method_name)
            call_args = inspect.getcallargs(
                method, None, *worker_ctx.args, **worker_ctx.kwargs
            )
            del call_args["self"]
            redacted = False

        call_args, truncated = utils.truncate(json.dumps(call_args))

        return {
            "call_args": call_args,
            "call_args_redacted": str(redacted),
            "call_args_truncated": str(truncated),
        }

    def get_exception_attributes(self, exc_info):
        """ Additional attributes to save alongside a worker exception.
        """
        exc_type, exc, _ = exc_info

        try:
            stacktrace = "\n".join(format_exception(*exc_info))
        except Exception:
            stacktrace = "Exception occurred on stacktrace formatting"

        return {
            "exception.stacktrace": stacktrace,
            "exception.expected": str(self.exception_was_expected(exc)),
        }

    def get_result_attributes(self, result):
        """ Attributes describing the entrypoint method result
        """
        return {"result": utils.safe_for_serialisation(result or "")}

    def get_status(self, result, exc_info):
        """ Span status for this entrypoint method
        """
        if exc_info:
            exc_type, exc, _ = exc_info

            if not self.exception_was_expected(exc):
                return Status(
                    StatusCode.ERROR,
                    description="{}: {}".format(type(exc).__name__, exc),
                )

        return Status(StatusCode.OK)


def adapter_factory(worker_ctx):
    adapter_class = adapter_types[type(worker_ctx.entrypoint)]
    return adapter_class(worker_ctx)


def worker_setup(tracer, wrapped, instance, args, kwargs):
    (worker_ctx,) = args

    adapter = adapter_factory(worker_ctx)
    ctx = extract(adapter.get_metadata())
    token = context.attach(ctx)

    span = tracer.start_span(
        adapter.get_span_name(),
        kind=trace.SpanKind.SERVER,
        attributes={"hostname": socket.gethostname()},
        start_time=_time_ns(),
    )
    activation = trace.use_span(
        span, record_exception=False, set_status_on_exception=False
    )
    activation.__enter__()
    active_spans[worker_ctx] = (activation, span, token, adapter)

    adapter.start_span(span)


def worker_result(tracer, wrapped, instance, args, kwargs):
    (worker_ctx, result, exc_info) = args

    activated = active_spans.get(worker_ctx)
    if not activated:
        # something went wrong when starting the span; nothing more to do
        return

    activation, span, token, adapter = activated

    adapter.end_span(span, result, exc_info)

    if exc_info is None:
        activation.__exit__(None, None, None)
    else:
        activation.__exit__(*exc_info)

    span.end(_time_ns())
    context.detach(token)


def instrument(tracer, entrypoint_adapters):

    adapter_config = DEFAULT_ADAPTERS.copy()
    adapter_config.update(entrypoint_adapters)
    for entrypoint_path, adapter_path in adapter_config.items():
        entrypoint_class = utils.import_by_path(entrypoint_path)
        adapter_class = utils.import_by_path(adapter_path)
        adapter_types[entrypoint_class] = adapter_class

    wrap_function_wrapper(
        "nameko.containers",
        "ServiceContainer._worker_setup",
        partial(worker_setup, tracer),
    )
    wrap_function_wrapper(
        "nameko.containers",
        "ServiceContainer._worker_result",
        partial(worker_result, tracer),
    )


def uninstrument():
    unwrap(nameko.containers.ServiceContainer, "worker_setup")
    unwrap(nameko.containers.ServiceContainer, "worker_result")

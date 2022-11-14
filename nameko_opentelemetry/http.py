# -*- coding: utf-8 -*-
import inspect
from functools import partial

import nameko.web.server
from nameko.web.handlers import Response
from nameko.web.server import HTTPException
from opentelemetry import trace
from opentelemetry.instrumentation import wsgi
from opentelemetry.instrumentation.utils import http_status_to_status_code, unwrap
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace.status import Status, StatusCode
from wrapt import wrap_function_wrapper

from nameko_opentelemetry import utils
from nameko_opentelemetry.entrypoints import EntrypointAdapter
from nameko_opentelemetry.scrubbers import scrub


class HttpEntrypointAdapter(EntrypointAdapter):
    """Implemented according to
    https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md
    """

    def get_metadata(self, worker_ctx):
        # TODO: why doesn't http entrypoint populate context data?
        # alternatively, why do AMQP (and gRPC?) extensions feel they can turn
        # _all_ amqp headers into context data?
        return worker_ctx.args[0].headers

    def get_span_name(self, worker_ctx):
        return worker_ctx.entrypoint.url

    def request(self, worker_ctx):
        entrypoint = worker_ctx.entrypoint

        method = getattr(entrypoint.container.service_cls, entrypoint.method_name)
        call_args = inspect.getcallargs(
            method, None, *worker_ctx.args, **worker_ctx.kwargs
        )

        return call_args.get("request")

    def get_attributes(self, worker_ctx):

        attributes = super().get_attributes(worker_ctx)
        attributes.pop("call_args", None)
        attributes.pop("call_args_redacted", None)
        attributes.pop("call_args_truncated", None)

        request = self.request(worker_ctx)
        data = request.data or request.form

        attributes.update(wsgi.collect_request_attributes(request.environ))

        if self.config.get("send_headers"):
            headers = {}
            for key, value in request.environ.items():
                key = str(key)
                if key.startswith("HTTP_") and key not in (
                    "HTTP_CONTENT_TYPE",
                    "HTTP_CONTENT_LENGTH",
                ):
                    headers[key[5:].lower()] = str(value)
                elif key in ("CONTENT_TYPE", "CONTENT_LENGTH"):
                    headers[key.lower()] = str(value)

            attributes.update(
                {
                    "request.headers": utils.serialise_to_string(
                        scrub(headers, self.config)
                    )
                }
            )

        if self.config.get("send_request_payloads"):
            response, truncated = utils.truncate(
                utils.serialise_to_string(scrub(data, self.config)),
                max_len=self.config.get("truncate_max_length"),
            )
            attributes.update(
                {
                    "request.data": response,
                    "request.data_truncated": str(truncated),
                }
            )

        return attributes

    def get_result_attributes(self, worker_ctx, result):
        """Return serialisable result data"""
        if not isinstance(result, Response):
            if isinstance(result, tuple):
                if len(result) == 3:
                    status, headers, payload = result
                else:
                    status, payload = result
                    headers = {}
            else:
                payload = result
                status = 200
                headers = {}

            result = Response(payload, headers=headers.items(), status=status)

        attributes = {
            "response.content_type": result.content_type,
            SpanAttributes.HTTP_RESPONSE_CONTENT_LENGTH: result.content_length,
            SpanAttributes.HTTP_STATUS_CODE: result.status_code,
        }

        if self.config.get("send_response_payloads"):

            response, truncated = utils.truncate(
                scrub(result.get_data(), self.config),
                max_len=self.config.get("truncate_max_length"),
            )
            attributes.update(
                {"response.data": response, "response.data_truncated": str(truncated)}
            )

        return attributes

    def get_status(self, worker_ctx, result, exc_info):
        if exc_info:
            exc_type, exc, _ = exc_info

            return Status(
                StatusCode.ERROR,
                description="{}: {}".format(type(exc).__name__, exc),
            )

        result_attributes = self.get_result_attributes(worker_ctx, result)
        return Status(
            http_status_to_status_code(
                result_attributes[SpanAttributes.HTTP_STATUS_CODE]
            )
        )


def wsgi_app_call(tracer, config, wrapped, instance, args, kwargs):
    environ, start_response = args
    try:
        instance.url_map.bind_to_environ(environ).match()
    except HTTPException as exc:
        span = tracer.start_span(
            wsgi.get_default_span_name(environ), kind=trace.SpanKind.SERVER
        )
        span.set_status(Status(StatusCode.ERROR, description=str(exc)))
        with trace.use_span(span, end_on_exit=True):
            return wrapped(*args, **kwargs)
    else:
        return wrapped(*args, **kwargs)


def instrument(tracer, config):
    wrap_function_wrapper(
        "nameko.web.server",
        "WsgiApp.__call__",
        partial(wsgi_app_call, tracer, config),
    )


def uninstrument():
    unwrap(nameko.web.server.WsgiApp, "__call__")

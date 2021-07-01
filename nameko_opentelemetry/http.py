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


class HttpEntrypointAdapter(EntrypointAdapter):
    """ Implemented according to https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md
    """

    def get_metadata(self):
        # TODO: why doesn't http entrypoint populate context data? alternatively, why do AMQP (and gRPC?) extensions feel they can turn _all_ amqp headers into context data?
        return self.worker_ctx.args[0].headers

    def get_span_name(self):
        return self.worker_ctx.entrypoint.url

    @property
    def request(self):
        worker_ctx = self.worker_ctx
        entrypoint = worker_ctx.entrypoint

        method = getattr(entrypoint.container.service_cls, entrypoint.method_name)
        call_args = inspect.getcallargs(
            method, None, *worker_ctx.args, **worker_ctx.kwargs
        )

        return call_args.get("request")

    def get_call_args_attributes(self):

        request = self.request
        data = request.data or request.form

        attributes = wsgi.collect_request_attributes(request.environ)
        attributes.update(
            {
                "request.data": utils.serialise_to_string(
                    data
                ),  # do we want to send this?
                "request.headers": utils.serialise_to_string(
                    self.get_headers(request.environ)
                ),  # again do we want to send this? + scrubbing?
            }
        )

        return attributes

    def get_result_attributes(self, result):
        """ Return serialisable result data
        """
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

        response, truncated = utils.truncate(result.get_data())

        return {
            "response.content_type": result.content_type,
            "response.data": response,  # do we want to send this?
            "response.data_truncated": str(truncated),
            SpanAttributes.HTTP_RESPONSE_CONTENT_LENGTH: result.content_length,
            SpanAttributes.HTTP_STATUS_CODE: result.status_code,
        }

    def get_headers(self, environ):
        """ Return only proper HTTP headers
        """
        for key, value in environ.items():
            key = str(key)
            if key.startswith("HTTP_") and key not in (
                "HTTP_CONTENT_TYPE",
                "HTTP_CONTENT_LENGTH",
            ):
                yield key[5:].lower(), str(value)
            elif key in ("CONTENT_TYPE", "CONTENT_LENGTH"):
                yield key.lower(), str(value)

    def get_status(self, result, exc_info):
        if exc_info:
            exc_type, exc, _ = exc_info

            return Status(
                StatusCode.ERROR, description="{}: {}".format(type(exc).__name__, exc),
            )

        result_attributes = self.get_result_attributes(result)
        return Status(
            http_status_to_status_code(
                result_attributes[SpanAttributes.HTTP_STATUS_CODE]
            )
        )


def wsgi_app_call(tracer, wrapped, instance, args, kwargs):
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


def instrument(tracer):
    wrap_function_wrapper(
        "nameko.web.server", "WsgiApp.__call__", partial(wsgi_app_call, tracer),
    )


def uninstrument():
    unwrap(nameko.web.server.WsgiApp, "__call__")
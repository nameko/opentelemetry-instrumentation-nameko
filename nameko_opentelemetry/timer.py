# -*- coding: utf-8 -*-
from nameko_opentelemetry.entrypoints import EntrypointAdapter


class TimerEntrypointAdapter(EntrypointAdapter):
    """Adapter customisation for Timer entrypoints."""

    def get_attributes(self, worker_ctx):
        attrs = super().get_attributes(worker_ctx)

        attrs.update(
            {
                "nameko.timer.interval": worker_ctx.entrypoint.interval,
                "nameko.timer.eager": worker_ctx.entrypoint.eager,
            }
        )
        return attrs

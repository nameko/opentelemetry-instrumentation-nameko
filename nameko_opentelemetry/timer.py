# -*- coding: utf-8 -*-
from nameko_opentelemetry.entrypoints import EntrypointAdapter


class TimerEntrypointAdapter(EntrypointAdapter):
    """ Adapter customisation for Timer entrypoints.
    """

    def get_attributes(self):
        attrs = super().get_attributes()

        attrs.update(
            {
                "nameko.timer.interval": self.worker_ctx.entrypoint.interval,
                "nameko.timer.eager": self.worker_ctx.entrypoint.eager,
            }
        )
        return attrs

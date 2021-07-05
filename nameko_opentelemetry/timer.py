from nameko_opentelemetry.entrypoints import EntrypointAdapter


class TimerEntrypointAdapter(EntrypointAdapter):
    def get_attributes(self):
        attrs = super().get_attributes()

        attrs.update(
            {
                "nameko.timer.interval": self.worker_ctx.entrypoint.interval,
                "nameko.timer.eager": self.worker_ctx.entrypoint.eager,
            }
        )
        return attrs

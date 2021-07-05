from nameko_opentelemetry.entrypoints import EntrypointAdapter


class TimerEntrypointAdapter(EntrypointAdapter):
    def get_common_attributes(self):
        attrs = super().get_common_attributes()

        attrs.update(
            {
                "nameko.timer.interval": self.worker_ctx.entrypoint.interval,
                "nameko.timer.eager": self.worker_ctx.entrypoint.eager,
            }
        )
        return attrs

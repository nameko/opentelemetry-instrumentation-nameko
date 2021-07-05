import pytest
from nameko.testing.services import entrypoint_waiter
from nameko.timer import timer


class TestServerAttributes:
    @pytest.fixture
    def container(self, container_factory, rabbit_config):
        class Service:
            name = "service"

            @timer(interval=0.001, eager=True)
            def method(self):
                return "OK"

        container = container_factory(Service)
        container.start()

        return container

    def test_attributes(self, container, memory_exporter):

        with entrypoint_waiter(container, "method"):
            pass

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1

        attributes = spans[0].attributes
        assert attributes["nameko.timer.interval"] == 0.001
        assert attributes["nameko.timer.eager"] is True

"""Exercise stream lifetime boundaries through PySpark's real timeout conversion."""

# ruff: noqa: E402 -- imports intentionally follow pytest.importorskip.

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("pyspark")
from pyspark.sql.streaming import StreamingQuery

from src.module3 import stream
from src.module3.config import Config


class FakeQuery:
    """Use PySpark's seconds-to-milliseconds adapter against a fake JVM query."""

    awaitTermination = StreamingQuery.awaitTermination

    def __init__(self, events):
        self.id = "timeout-query"
        self.name = "timeout-test"
        self.isActive = True
        self.recentProgress = [{"batchId": 1}]
        self.waits = []
        self.failure = None
        self.stop_error = None
        self.stop_event = None
        self.events = events
        self.on_wait = Mock()
        self._jsq = SimpleNamespace(awaitTermination=self._await_milliseconds)

    def _await_milliseconds(self, milliseconds):
        self.waits.append(milliseconds)
        assert milliseconds > 0, "Spark requires a strictly positive millisecond timeout"
        self.on_wait()
        return not self.isActive

    def exception(self):
        return self.failure

    def terminate(self):
        self.isActive = False

    def stop(self):
        assert not self.stop_event.is_set()
        self.events.append("stop")
        self.isActive = False
        if self.stop_error is not None:
            raise self.stop_error


@pytest.fixture
def runtime(monkeypatch, tmp_path):
    events = []
    query = FakeQuery(events)
    writer = Mock()
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.foreachBatch.return_value = writer
    writer.queryName.return_value = writer
    writer.trigger.return_value = writer
    writer.start.return_value = query
    monkeypatch.setattr(stream, "from_kafka", Mock(return_value=object()))
    monkeypatch.setattr(stream, "build_window_counts", Mock(return_value=SimpleNamespace(writeStream=writer)))
    monkeypatch.setattr(stream, "ensure_checkpoint", Mock(return_value="file:///test/checkpoint"))

    # Anchor the deadline at zero so exact millisecond boundaries are not lost
    # to subtraction of a large artificial clock origin.
    clock = Mock(side_effect=[-10.0, -0.5, 0.0])
    sleep = Mock(side_effect=AssertionError("bounded streaming must not sleep"))
    monkeypatch.setattr(stream, "time", SimpleNamespace(monotonic=clock, sleep=sleep))
    logger = Mock()
    logger.start.side_effect = lambda: events.append("start")
    logger.is_alive.return_value = False

    def progress_logger(_query, _output, stop_event, _publisher, _errors, **_kwargs):
        query.stop_event = stop_event
        return logger

    def join():
        assert query.stop_event.is_set()
        assert events[-1] == "stop"
        events.append("join")

    logger.join.side_effect = join
    monkeypatch.setattr(stream, "_progress_logger", progress_logger)

    def run(duration_seconds=10, **kwargs):
        return stream.run_stream(
            object(), Config(), tmp_path, "timeout-test", duration_seconds=duration_seconds, **kwargs
        )

    yield SimpleNamespace(query=query, clock=clock, sleep=sleep, run=run)

    # Every exit path must stop the query before signalling and joining its logger.
    assert events == ["start", "stop", "join"]
    assert query.stop_event.is_set()
    assert not query.isActive


@pytest.mark.parametrize(
    "remaining, expected_milliseconds",
    [
        (2.0, [1000]),
        (0.5, [500]),
        (0.0, []),
        (-0.0001, []),
        (0.0005, []),
        (0.000999999, []),
        (0.001, [1]),
        (0.001000001, [1]),
    ],
    ids=["poll-cap", "positive", "deadline", "expired", "sub-ms", "below-one-ms", "one-ms", "above-one-ms"],
)
def test_bounded_wait_respects_millisecond_deadline(runtime, remaining, expected_milliseconds):
    runtime.clock.side_effect = [-10.0, -remaining, 0.0]

    result = runtime.run()

    assert runtime.query.waits == expected_milliseconds
    assert result["last_progress"] == {"batchId": 1}
    runtime.sleep.assert_not_called()


def test_query_can_terminate_normally_before_deadline(runtime):
    runtime.query.on_wait.side_effect = runtime.query.terminate

    result = runtime.run()

    assert runtime.query.waits == [500]
    assert result["last_progress"] == {"batchId": 1}
    assert "interrupted" not in result


@pytest.mark.parametrize("failure_path", ["await", "inactive"])
def test_query_failure_propagates_and_cleans_up(runtime, failure_path):
    failure = RuntimeError("query failed")
    if failure_path == "await":
        runtime.query.on_wait.side_effect = failure
    else:
        runtime.query.failure = failure
        runtime.query.on_wait.side_effect = runtime.query.terminate

    with pytest.raises(RuntimeError, match="query failed") as caught:
        runtime.run()

    assert caught.value is failure
    assert runtime.query.waits == [500]


def test_interrupted_bounded_stream_cleans_up(runtime):
    runtime.query.on_wait.side_effect = KeyboardInterrupt

    assert runtime.run()["interrupted"] is True
    assert runtime.query.waits == [500]


def test_logger_is_joined_even_when_query_stop_fails(runtime):
    runtime.clock.side_effect = [-10.0, 0.0]
    failure = RuntimeError("stop failed")
    runtime.query.stop_error = failure

    with pytest.raises(RuntimeError, match="stop failed") as caught:
        runtime.run()

    assert caught.value is failure
    assert runtime.query.waits == []


@pytest.mark.parametrize("outcome", ["normal", "failure", "interrupt"])
def test_unlimited_stream_preserves_interruptible_polling(runtime, outcome):
    runtime.clock.side_effect = [-10.0]

    def poll(seconds):
        assert seconds == 1.0
        if outcome == "interrupt":
            raise KeyboardInterrupt
        runtime.query.isActive = False
        if outcome == "failure":
            runtime.query.failure = RuntimeError("unlimited query failed")

    runtime.sleep.side_effect = poll

    if outcome == "failure":
        with pytest.raises(RuntimeError, match="unlimited query failed"):
            runtime.run(duration_seconds=None)
    else:
        result = runtime.run(duration_seconds=None)
        assert result.get("interrupted", False) is (outcome == "interrupt")

    runtime.sleep.assert_called_once_with(1.0)
    assert runtime.query.waits == []


def test_completion_driven_stream_keeps_reconciliation_path(runtime):
    runtime.clock.side_effect = [-10.0, -9.0]
    receipt = {"consumed_records": 5000}
    completion = Mock(return_value=receipt)

    result = runtime.run(until_complete=completion)

    assert result["completion"] == receipt
    completion.assert_called_once_with(runtime.query)
    assert runtime.query.waits == []
    runtime.sleep.assert_not_called()


def test_completion_driven_stream_keeps_hard_deadline(runtime):
    runtime.clock.side_effect = [-10.0, 0.0]
    completion = Mock()

    with pytest.raises(TimeoutError, match="deadline has expired"):
        runtime.run(until_complete=completion)

    completion.assert_not_called()
    assert runtime.query.waits == []

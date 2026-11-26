"""Redis-backed broker for multi-instance SSE streaming.

Uses Redis pub/sub for live event broadcast and Redis Lists for replay storage.
The replay buffer (Redis List) stores resumable events with a TTL so they
auto-expire without a cleanup loop. On reconnect, LRANGE fetches missed events.

Event sequence counters are stored as Redis INCR keys for O(1) cross-instance
access (instead of deriving from the replay buffer with O(N) LRANGE).
"""

import asyncio
import contextlib
import hashlib
import json
import random
import time
from collections import OrderedDict
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import structlog
from redis import RedisError
from redis.asyncio.client import PubSub
from redis.exceptions import ResponseError

from aegra_api.core.active_runs import active_runs, explicit_run_cancellations
from aegra_api.core.redis_manager import redis_manager
from aegra_api.core.serializers import GeneralSerializer
from aegra_api.models.enums import RunCancellationAction
from aegra_api.services.base_broker import (
    REPLAY_RETENTION_SECONDS,
    BaseBrokerManager,
    BaseRunBroker,
    ReplayGapError,
    _is_terminal_payload,
)
from aegra_api.settings import settings
from aegra_api.utils import generate_event_id

logger = structlog.getLogger(__name__)

_serializer = GeneralSerializer()

# Every write refreshes the TTL, so the buffer expires this long after the last
# event, matching the in-memory post-completion window.
_REPLAY_TTL_SECONDS = REPLAY_RETENTION_SECONDS
# Max events in the replay buffer (prevents unbounded growth)
_REPLAY_MAX_EVENTS = 10_000

# Reconnect backoff for Redis pub/sub listeners
_BACKOFF_BASE = 0.5
_BACKOFF_MAX = 30.0
_BACKOFF_FACTOR = 2.0

# Bounded retry for the write path (put). The read path (aiter) retries
# RedisError indefinitely; the write path must be bounded so a producer can't
# block forever, but it should still retry a transient blip rather than drop the
# event — a dropped event is a permanently lost SSE token.
_PUT_MAX_ATTEMPTS = 3

# Stored events read per LRANGE: what a reader holds follows this, not the run's length.
_REPLAY_PAGE_EVENTS = 16
# A follower re-reads the buffer this often without a pub/sub wake-up (a PUBLISH can fail).
_LIVE_RECHECK_SECONDS = 5.0


def _serialize_payload(payload: Any) -> str:
    """Serialize an event payload to a JSON string for Redis transport."""
    return json.dumps(payload, default=_serializer.serialize)


def _deserialize_payload(raw: Any) -> Any:
    """Convert JSON-deserialized data back to expected Python types.

    Event payloads are tuples like ("values", {...}). JSON has no tuple type,
    so they arrive as lists. We convert top-level lists with a string first
    element back to tuples.
    """
    if isinstance(raw, list) and len(raw) >= 1 and isinstance(raw[0], str):
        return tuple(raw)
    return raw


_TAIL_POSITION = """
local tail = redis.call('LINDEX', KEYS[1], -1)
local epoch, ordinal = string.match(tail or '', '^{"position":"(%x+):(%d+)",')
"""

_APPEND = (
    _TAIL_POSITION
    + """
local message, new_epoch, cap, ttl = unpack(ARGV)
local position = (epoch or new_epoch) .. ':' .. ((ordinal or 0) + 1)
local length = redis.call('RPUSH', KEYS[1], '{"position":"' .. position .. '",' .. string.sub(message, 2))
redis.call('LTRIM', KEYS[1], -tonumber(cap), -1)
redis.call('EXPIRE', KEYS[1], ttl)
redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], ttl)
return {length, position}
"""
)

# Older writers increment the counter for every cache append and separately for ID allocation.
# On an unpositioned page this bounds how far the head could have moved.
_READ_PAGE = (
    _TAIL_POSITION
    + """
local length = redis.call('LLEN', KEYS[1])
local counter = tonumber(redis.call('GET', KEYS[2]) or '0')
local index, old_length, old_counter, cap, page, previous_epoch, previous_ordinal, anchor = unpack(ARGV)
local first
if epoch and epoch == previous_epoch then
    first = length - (ordinal - previous_ordinal)
elseif previous_epoch ~= '' then
    if anchor == '' then return {-2, length, counter, {}} end
    local at = redis.call('LPOS', KEYS[1], anchor)
    first = at and at + 1 or -1
else
    local trims = length < tonumber(cap) and 0 or math.max(0, old_length + counter - old_counter - cap)
    first = math.max(0, index - trims)
    if trims == 0 and anchor ~= '' and redis.call('LINDEX', KEYS[1], index - 1) ~= anchor then
        first = -1
    end
end
if first < 0 or first > length then return {-1, length, counter, {}} end
return {first, length, counter, redis.call('LRANGE', KEYS[1], first, first + page - 1)}
"""
)


@dataclass
class _ReplayPlace:
    thread_run_id: str | None = None
    index: int = 0
    length: int = 0
    counter: int = 0
    anchor: str = ""
    epoch: str = ""
    ordinal: int = 0
    epochs: OrderedDict[str, int] = field(default_factory=OrderedDict)
    recent: OrderedDict[str, None] = field(default_factory=OrderedDict)
    legacy: OrderedDict[bytes, None] = field(default_factory=OrderedDict)
    ended: bool = False

    def consume(self, raw: str, data: dict[str, Any]) -> tuple[str, Any] | None:
        self.anchor = raw
        self.index += 1
        position = data.get("position")
        if position:
            self.epoch, n = position.split(":")
            self.ordinal = int(n)
            if self.ordinal <= self.epochs.get(self.epoch, 0):
                return None
            self.epochs[self.epoch] = self.ordinal
            if len(self.epochs) > _REPLAY_MAX_EVENTS:
                self.epochs.popitem(last=False)
            delivery = data["delivery_id"]
            if delivery in self.recent:
                self.recent.move_to_end(delivery)
                return None
            self.recent[delivery] = None
            if len(self.recent) > 128:
                self.recent.popitem(last=False)
        else:
            self.epoch, self.ordinal = "", 0
            digest = hashlib.sha256(raw.encode()).digest()
            if digest in self.legacy:
                self.legacy.move_to_end(digest)
                return None
            self.legacy[digest] = None
            if len(self.legacy) > _REPLAY_MAX_EVENTS:
                self.legacy.popitem(last=False)
        payload = _deserialize_payload(data["payload"])
        self.ended = _is_terminal_payload(payload)
        event_id = data["event_id"]
        if self.thread_run_id is not None:
            suffix = delivery if position else digest.hex()
            event_id = f"{self.thread_run_id}:delivery:{suffix}"
        return event_id, payload


def _backoff_delay(attempt: int) -> float:
    """Calculate exponential backoff delay with jitter."""
    delay = min(_BACKOFF_BASE * (_BACKOFF_FACTOR**attempt), _BACKOFF_MAX)
    jitter = random.uniform(0, delay * 0.25)  # noqa: S311  # nosec B311
    return delay + jitter


class RedisRunBroker(BaseRunBroker):
    """Broker for a single run backed by Redis pub/sub + Redis Lists.

    Producer: RPUSH to list (replay buffer) + PUBLISH to channel (live).
    Consumer: LRANGE for replay, then SUBSCRIBE for live events.
    """

    def __init__(self, run_id: str, channel: str, cache_key: str, counter_key: str) -> None:
        self.run_id = run_id
        self._channel = channel
        self._cache_key = cache_key
        self._counter_key = counter_key
        self._finished = asyncio.Event()
        self._observed_buffer = False
        self._observed_cap = False
        # Serializes writes for this run so concurrent put() calls (e.g. a
        # cancel/error signal racing a stream write) keep cache-then-publish
        # ordering — without it, a backoff sleep on one put could let a later
        # event's publish overtake an earlier one.
        self._write_lock = asyncio.Lock()

    async def put(self, event_id: str, payload: Any, *, resumable: bool = True) -> None:
        """Append an event to the replay buffer and publish it to live subscribers.

        Writes are retried with bounded backoff (see ``_write_with_retry``), so
        delivery is **at-least-once**: in the rare case a write times out *after*
        Redis already applied it, a retry may re-deliver the event. Retry copies
        share one delivery ID. Each physical append has its own position.
        """
        if self._finished.is_set():
            logger.warning(f"Attempted to put event {event_id} into finished broker for run {self.run_id}")
            return

        envelope = {
            "event_id": event_id,
            "delivery_id": uuid4().hex,
            "payload": json.loads(_serialize_payload(payload)),
        }
        message = json.dumps(envelope)

        is_end = isinstance(payload, tuple) and len(payload) >= 1 and payload[0] == "end"

        async with self._write_lock:
            # Cache and publish are retried independently so a publish failure
            # never re-runs the cache pipeline (which would double-write the
            # replay buffer and double-increment the sequence counter).
            operation = "cache"
            try:
                if resumable:
                    length, position = await self._write_with_retry(self._cache_event, message)
                    if not self._observed_buffer:
                        logger.info(
                            "run_replay_buffer", run_id=self.run_id, milestone="started", cap=_REPLAY_MAX_EVENTS
                        )
                        self._observed_buffer = True
                    if length >= _REPLAY_MAX_EVENTS and not self._observed_cap:
                        logger.info(
                            "run_replay_buffer", run_id=self.run_id, milestone="reached_cap", cap=_REPLAY_MAX_EVENTS
                        )
                        self._observed_cap = True
                    message = '{"position":"' + position + '",' + message[1:]

                operation = "publish"
                await self._write_with_retry(self._publish_event, message)

                if is_end:
                    self._finished.set()
            except RedisError as e:
                logger.error(
                    f"Redis {operation} write failed for run {self.run_id} after {_PUT_MAX_ATTEMPTS} attempts: {e}"
                )
                # Even if Redis fails, mark finished for end events so aiter() can exit
                # rather than looping forever waiting for an end event that won't arrive.
                if is_end:
                    self._finished.set()

    async def _cache_event(self, message: str) -> tuple[int, str]:
        append = redis_manager.get_client().register_script(_APPEND)
        length, position = await append(
            keys=[self._cache_key, self._counter_key],
            args=[message, uuid4().hex, _REPLAY_MAX_EVENTS, _REPLAY_TTL_SECONDS],
        )
        return length, position

    async def _publish_event(self, message: str) -> None:
        """Broadcast the event to live subscribers."""
        client = redis_manager.get_client()
        await client.publish(self._channel, message)

    async def _write_with_retry[T](self, op: Callable[[str], Awaitable[T]], message: str) -> T:
        """Run a Redis write with bounded exponential backoff.

        Mirrors the retry/backoff the read path (aiter) already applies, so a
        transient client-side blip (e.g. socket timeout) is retried instead of
        dropping the event. Bounded by ``_PUT_MAX_ATTEMPTS`` so a producer never
        blocks indefinitely; the final ``RedisError`` propagates to ``put()``.
        """
        attempt = 0
        while True:
            try:
                return await op(message)
            except RedisError as e:
                attempt += 1
                if attempt >= _PUT_MAX_ATTEMPTS:
                    raise
                delay = _backoff_delay(attempt)
                logger.warning(
                    f"Redis write failed for run {self.run_id}, retrying in {delay:.1f}s: {e}",
                    attempt=attempt,
                )
                await asyncio.sleep(delay)

    async def aiter(self) -> AsyncIterator[tuple[str, Any]]:
        attempt = 0
        while not self._finished.is_set():
            try:
                async for event_id, payload in self._subscribe_and_listen():
                    attempt = 0
                    yield event_id, payload
                # Clean exit from _subscribe_and_listen (end event or finished)
                break
            except RedisError as e:
                attempt += 1
                delay = _backoff_delay(attempt)
                logger.warning(
                    f"Redis pub/sub error for run {self.run_id}, retrying in {delay:.1f}s: {e}",
                    attempt=attempt,
                )
                await asyncio.sleep(delay)

    async def _subscribe_and_listen(self) -> AsyncIterator[tuple[str, Any]]:
        """Subscribe to the channel and yield events until end or disconnect."""
        client = redis_manager.get_client()
        pubsub = client.pubsub()
        await pubsub.subscribe(self._channel)

        # After subscribing, check if the run already ended (closes the race where
        # the end event was published before we subscribed on this instance).
        end_already_in_buffer = await self._check_end_in_buffer()

        last_delivery: str | None = None
        try:
            while True:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=0.5,
                )
                if message is None:
                    if self._finished.is_set() or end_already_in_buffer:
                        break
                    continue

                if message["type"] != "message":
                    continue

                data = json.loads(message["data"])
                event_id: str = data["event_id"]

                delivery = data.get("delivery_id") or message["data"]
                if delivery == last_delivery:
                    continue
                last_delivery = delivery

                payload = _deserialize_payload(data["payload"])

                yield event_id, payload

                if isinstance(payload, tuple) and len(payload) >= 1 and payload[0] == "end":
                    self._finished.set()
                    break
        finally:
            await pubsub.unsubscribe(self._channel)
            await pubsub.aclose()

    async def _check_end_in_buffer(self) -> bool:
        """Check if an 'end' event is already in the replay buffer.

        Used after subscribing to detect runs that finished before we subscribed,
        preventing infinite loops in cross-instance consumer scenarios.
        """
        try:
            client = redis_manager.get_client()
            raw_messages = await client.lrange(self._cache_key, -1, -1)  # type: ignore[invalid-await]
            if raw_messages:
                data = json.loads(raw_messages[0])
                payload = _deserialize_payload(data["payload"])
                if isinstance(payload, tuple) and len(payload) >= 1 and payload[0] == "end":
                    self._finished.set()
                    return True
        except RedisError as e:
            logger.warning(f"Failed checking replay buffer for end event for run {self.run_id}: {e}")
        return False

    async def replay(self, last_event_id: str | None) -> list[tuple[str, Any]]:
        try:
            client = redis_manager.get_client()
            raw_messages = await client.lrange(self._cache_key, 0, _REPLAY_MAX_EVENTS - 1)  # type: ignore[invalid-await]
        except RedisError as e:
            logger.error(f"Redis replay failed for run {self.run_id}: {e}")
            return []

        if not raw_messages:
            return []

        all_events: list[tuple[str, Any]] = []
        events_after: list[tuple[str, Any]] = []
        found_last = last_event_id is None
        previous_delivery: str | None = None
        for raw in raw_messages:
            data = json.loads(raw)
            event_id: str = data["event_id"]

            delivery = data.get("delivery_id") or raw
            if delivery == previous_delivery:
                continue
            previous_delivery = delivery

            payload = _deserialize_payload(data["payload"])
            all_events.append((event_id, payload))

            if not found_last:
                if event_id == last_event_id:
                    found_last = True
                continue

            events_after.append((event_id, payload))

        # If last_event_id was not found in the buffer, return all events
        if not found_last:
            return all_events

        return events_after

    def events(self, *, follow: bool) -> AsyncGenerator[tuple[str, Any], None]:
        return self._events(_ReplayPlace(), follow=follow)

    def thread_events(self, *, follow: bool) -> AsyncGenerator[tuple[str, Any], None]:
        return self._events(_ReplayPlace(thread_run_id=self.run_id), follow=follow)

    async def _events(self, place: _ReplayPlace, *, follow: bool) -> AsyncGenerator[tuple[str, Any], None]:
        """Page stored events and follow indexed live publications, backfilling gaps from the list."""
        if not follow:
            try:
                async for event in self._stored(place, follow=False):
                    yield event
            except RedisError as e:
                raise ReplayGapError(f"Redis replay of run {self.run_id} failed: {e}") from e
            return

        pubsub: PubSub | None = None
        attempt = 0
        finished_wait = asyncio.create_task(self._finished.wait())
        try:
            while True:
                # Taken before the read, so a finish that lands during it still gets one more read.
                finished = self._finished.is_set()
                try:
                    async for event in self._stored(place, follow=True):
                        attempt = 0
                        yield event
                    if place.ended or finished:
                        return
                    if pubsub is None:
                        # Subscribing after the backlog keeps a long one out of Redis's pub/sub buffer.
                        pubsub = redis_manager.get_client().pubsub()
                        await pubsub.subscribe(self._channel)
                        continue
                    while not self._finished.is_set():
                        pending = asyncio.create_task(pubsub.get_message(timeout=_LIVE_RECHECK_SECONDS))
                        try:
                            await asyncio.wait((pending, finished_wait), return_when=asyncio.FIRST_COMPLETED)
                        finally:
                            if not pending.done():
                                pending.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await pending
                        if pending.cancelled():
                            break
                        message = pending.result()
                        if message is None:
                            break
                        if message["type"] != "message":
                            continue
                        data = json.loads(message["data"])
                        position = data.get("position")
                        if not place.epoch or position != f"{place.epoch}:{place.ordinal + 1}":
                            if position:
                                epoch, n = position.split(":")
                                if int(n) <= place.epochs.get(epoch, 0):
                                    continue
                            break
                        place.length = min(place.length + 1, _REPLAY_MAX_EVENTS)
                        place.index = place.length - 1
                        place.counter += 1
                        event = place.consume(message["data"], data)
                        if event is not None:
                            if place.ended and event[1][0] == "end":
                                self._finished.set()
                            yield event
                            if place.ended:
                                return
                except RedisError as e:
                    if finished:
                        raise ReplayGapError(f"Final replay of run {self.run_id} failed: {e}") from e
                    attempt += 1
                    delay = _backoff_delay(attempt)
                    logger.warning(
                        f"Redis follow failed for run {self.run_id}, re-reading the buffer in {delay:.1f}s: {e}",
                        attempt=attempt,
                    )
                    if pubsub is not None:
                        with contextlib.suppress(RedisError):
                            await pubsub.aclose()
                        pubsub = None
                    await asyncio.wait((finished_wait,), timeout=delay)
        finally:
            finished_wait.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await finished_wait
            if pubsub is not None:
                with contextlib.suppress(RedisError):
                    await pubsub.aclose()

    async def _read_page(self, place: _ReplayPlace) -> tuple[int, int, int, list[str]]:
        client = redis_manager.get_client()
        read = client.register_script(_READ_PAGE)
        args = [
            place.index,
            place.length,
            place.counter,
            _REPLAY_MAX_EVENTS,
            _REPLAY_PAGE_EVENTS,
            place.epoch,
            place.ordinal,
        ]
        keys = [self._cache_key, self._counter_key]
        legacy_anchor = "" if place.epoch else place.anchor
        first, length, counter, raws = await read(keys=keys, args=[*args, legacy_anchor])
        if first == -2:
            try:
                first, length, counter, raws = await read(keys=keys, args=[*args, place.anchor])
            except ResponseError as e:
                raise ReplayGapError(f"Redis cannot check run {self.run_id}'s place with LPOS: {e}") from e
        return first, length, counter, raws

    async def _stored(self, place: _ReplayPlace, *, follow: bool) -> AsyncIterator[tuple[str, Any]]:
        while True:
            first, length, counter, raws = await self._read_page(place)
            expired = length < place.length or counter < place.counter
            lost = first < 0 or expired
            if not lost and not place.epoch and first == 0 and place.index > 0 and place.anchor:
                client = redis_manager.get_client()
                try:
                    found = await client.lpos(self._cache_key, place.anchor)  # type: ignore[invalid-await]
                except ResponseError as e:
                    raise ReplayGapError(f"Redis cannot check run {self.run_id}'s place with LPOS: {e}") from e
                lost = found is None
            if lost:
                how = "expired" if expired else "trimmed past the reader"
                if not follow:
                    raise ReplayGapError(f"Replay of run {self.run_id} lost its place: buffer {how}")
                logger.warning(f"Replay of run {self.run_id} lost its place (buffer {how}), resuming at its head")
                place.index = place.length = place.counter = place.ordinal = 0
                place.anchor = place.epoch = ""
                continue
            place.index, place.length, place.counter = first, length, counter
            for raw in raws:
                event = place.consume(raw, json.loads(raw))
                if event is not None:
                    if place.ended and event[1][0] == "end":
                        self._finished.set()
                    yield event
                    if place.ended:
                        return
            if place.index >= length:
                return

    def mark_finished(self) -> None:
        self._finished.set()
        logger.debug(f"Redis broker for run {self.run_id} marked as finished")

    def is_finished(self) -> bool:
        return self._finished.is_set()


class RedisBrokerManager(BaseBrokerManager):
    """Manages RedisRunBroker instances with cross-instance cancel support.

    The local _brokers dict is a cache for status checks. Redis is the source
    of truth for event data. Cancel commands are broadcast via a dedicated
    pub/sub channel so any instance can cancel a run.
    """

    def __init__(self) -> None:
        self._brokers: dict[str, RedisRunBroker] = {}
        self._channel_prefix = settings.redis.REDIS_CHANNEL_PREFIX
        self._cache_prefix = f"{self._channel_prefix}cache:"
        self._counter_prefix = f"{self._channel_prefix}counter:"
        self._cancel_channel = f"{self._channel_prefix}cancel"
        self._listener_task: asyncio.Task[None] | None = None
        self._running: bool = False

    def _make_broker(self, run_id: str) -> RedisRunBroker:
        """Create a RedisRunBroker for a run_id."""
        channel = f"{self._channel_prefix}{run_id}"
        cache_key = f"{self._cache_prefix}{run_id}"
        counter_key = f"{self._counter_prefix}{run_id}"
        return RedisRunBroker(run_id, channel, cache_key, counter_key)

    def get_or_create_broker(self, run_id: str) -> RedisRunBroker:
        if run_id not in self._brokers:
            self._brokers[run_id] = self._make_broker(run_id)
            logger.debug(f"Created Redis broker for run {run_id}")
        return self._brokers[run_id]

    def get_broker(self, run_id: str) -> RedisRunBroker | None:
        """Get an existing broker from the local cache, or None."""
        return self._brokers.get(run_id)

    def cleanup_broker(self, run_id: str) -> None:
        broker = self._brokers.pop(run_id, None)
        if broker:
            broker.mark_finished()
            logger.debug(f"Cleaned up Redis broker for run {run_id}")

    def remove_broker(self, run_id: str) -> None:
        broker = self._brokers.pop(run_id, None)
        if broker:
            broker.mark_finished()
            logger.debug(f"Removed Redis broker for run {run_id}")

    async def start(self) -> None:
        """Start the cancel command listener."""
        self._running = True
        self._listener_task = asyncio.create_task(self._listen_for_cancel_commands())
        logger.info("Redis broker manager started", cancel_channel=self._cancel_channel)

    async def stop(self) -> None:
        """Stop the cancel command listener."""
        self._running = False
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listener_task
            self._listener_task = None
        logger.debug("Redis broker manager stopped")

    async def request_cancel(
        self,
        run_id: str,
        action: RunCancellationAction = "cancel",
        *,
        emit_end_event: bool = True,
    ) -> None:
        """Broadcast a cancel command via Redis pub/sub."""
        message = json.dumps(
            {
                "run_id": run_id,
                "action": action,
                "emit_end_event": emit_end_event,
            }
        )
        try:
            client = redis_manager.get_client()
            await client.publish(self._cancel_channel, message)
            logger.info(f"Published {action} command for run {run_id}")
        except RedisError as e:
            logger.error(f"Failed to publish {action} for run {run_id}: {e}")
            # Fall back to local execution - if the task is on this instance,
            # we can still cancel it even if Redis publish fails.
            await self._execute_cancel(run_id, emit_end_event=emit_end_event)

    async def get_event_sequence(self, run_id: str) -> int:
        """Read the current event sequence counter from Redis (O(1) GET)."""
        try:
            client = redis_manager.get_client()
            value = await client.get(f"{self._counter_prefix}{run_id}")
            if value is not None:
                return int(value)
        except (RedisError, ValueError) as e:
            logger.warning(f"Failed to read event counter for run {run_id}: {e}")
        return 0

    async def allocate_event_id(self, run_id: str) -> str:
        """Atomically allocate the next event sequence number and return the event_id.

        Uses Redis INCR which is atomic — two concurrent callers will always
        get different sequence numbers. This prevents the race condition where
        get_event_sequence + 1 could return the same value to two callers.
        """
        counter_key = f"{self._counter_prefix}{run_id}"
        try:
            client = redis_manager.get_client()
            seq = await client.incr(counter_key)
            await client.expire(counter_key, _REPLAY_TTL_SECONDS)
            return generate_event_id(run_id, int(seq))
        except RedisError as e:
            logger.warning(f"Failed to allocate event_id for run {run_id}: {e}")
            # Fallback: use timestamp-based ID (unique but not sequential)
            return generate_event_id(run_id, int(time.time() * 1000))

    async def _listen_for_cancel_commands(self) -> None:
        """Background task: subscribe to cancel channel with reconnect backoff."""
        attempt = 0
        while self._running:
            try:
                await self._subscribe_and_handle_cancels()
                attempt = 0
            except asyncio.CancelledError:
                break
            except RedisError as e:
                attempt += 1
                delay = _backoff_delay(attempt)
                logger.error(
                    f"Cancel listener disconnected, retrying in {delay:.1f}s: {e}",
                    attempt=attempt,
                )
                await asyncio.sleep(delay)

    async def _subscribe_and_handle_cancels(self) -> None:
        """Subscribe to cancel channel and process commands until stopped."""
        client = redis_manager.get_client()
        pubsub = client.pubsub()
        await pubsub.subscribe(self._cancel_channel)

        try:
            while self._running:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=0.5,
                )
                if message is None:
                    continue
                if message["type"] != "message":
                    continue

                try:
                    data = json.loads(message["data"])
                    emit_end_event = data.get("emit_end_event", True)
                    if not isinstance(emit_end_event, bool):
                        raise ValueError("emit_end_event must be a boolean")
                    await self._execute_cancel(data["run_id"], emit_end_event=emit_end_event)
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    logger.warning(f"Invalid cancel message: {e}")
        finally:
            await pubsub.unsubscribe(self._cancel_channel)
            await pubsub.aclose()

    async def _execute_cancel(self, run_id: str, *, emit_end_event: bool = True) -> None:
        """Cancel a locally-owned task and signal the broker."""
        task = active_runs.get(run_id)
        if task is None or task.done():
            return

        logger.info(f"Cancelling run {run_id} (local task found)")
        explicit_run_cancellations.add(run_id)
        task.cancel()

        broker = self.get_or_create_broker(run_id)
        if emit_end_event and not broker.is_finished():
            event_id = await self.allocate_event_id(run_id)
            await broker.put(event_id, ("end", {"status": "interrupted"}))
            # Do NOT call cleanup_broker here - execute_run's finally block
            # owns cleanup.  Leaving the finished broker in _brokers lets
            # signal_run_cancelled see is_finished() → True and skip the
            # duplicate end event.

    async def start_cleanup_task(self) -> None:
        """No-op. Redis TTL handles replay buffer expiry."""

    async def stop_cleanup_task(self) -> None:
        """No-op."""

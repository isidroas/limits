from __future__ import annotations

import functools
import random

import pytest

import limits.aio.strategies
from limits import (
    RateLimitItemPerDay,
    RateLimitItemPerHour,
    RateLimitItemPerMinute,
    RateLimitItemPerSecond,
)
from limits.storage import storage_from_string
from limits.strategies import (
    FixedWindowRateLimiter,
    MovingWindowRateLimiter,
    SlidingWindowCounterRateLimiter,
)
from tests.utils import (
    all_storage,
    async_all_storage,
    async_moving_window_storage,
    async_sliding_window_counter_storage,
    moving_window_storage,
    sliding_window_counter_storage,
)

benchmark_limits = pytest.mark.parametrize(
    "limit",
    [
        RateLimitItemPerSecond(50),
        RateLimitItemPerMinute(500),
        RateLimitItemPerHour(10000),
        RateLimitItemPerDay(100000),
    ],
    ids=lambda limit: str(limit),
)


def hit_window(strategy, storage, limit):
    uid = int(random.random() * 100)
    strategy(storage).hit(limit, uid)


def hit_window_async(event_loop, strategy, storage, limit):
    uid = int(random.random() * 100)
    event_loop.run_until_complete(strategy(storage).hit(limit, uid))


@all_storage
@benchmark_limits
@pytest.mark.benchmark(group="fixed-window")
def test_fixed_window(benchmark, uri, args, limit, fixture):
    benchmark(
        functools.partial(
            hit_window, FixedWindowRateLimiter, storage_from_string(uri, **args), limit
        )
    )


@sliding_window_counter_storage
@benchmark_limits
@pytest.mark.benchmark(group="sliding-window-counter")
def test_sliding_window_counter(benchmark, uri, args, limit, fixture):
    benchmark(
        functools.partial(
            hit_window,
            SlidingWindowCounterRateLimiter,
            storage_from_string(uri, **args),
            limit,
        )
    )


@moving_window_storage
@benchmark_limits
@pytest.mark.benchmark(group="moving-window")
def test_moving_window(benchmark, uri, args, limit, fixture):
    benchmark(
        functools.partial(
            hit_window, MovingWindowRateLimiter, storage_from_string(uri, **args), limit
        )
    )


@async_all_storage
@benchmark_limits
@pytest.mark.benchmark(group="async-fixed-window")
def test_fixed_window_async(event_loop, benchmark, uri, args, limit, fixture):
    benchmark(
        functools.partial(
            hit_window_async,
            event_loop,
            limits.aio.strategies.FixedWindowRateLimiter,
            storage_from_string(uri, **args),
            limit,
        )
    )


@async_moving_window_storage
@benchmark_limits
@pytest.mark.benchmark(group="async-moving-window")
def test_moving_window_async(event_loop, benchmark, uri, args, limit, fixture):
    benchmark(
        functools.partial(
            hit_window_async,
            event_loop,
            limits.aio.strategies.MovingWindowRateLimiter,
            storage_from_string(uri, **args),
            limit,
        )
    )


@async_sliding_window_counter_storage
@benchmark_limits
@pytest.mark.benchmark(group="async-sliding-window-counter")
def test_sliding_window_counter_async(event_loop, benchmark, uri, args, limit, fixture):
    benchmark(
        functools.partial(
            hit_window_async,
            event_loop,
            limits.aio.strategies.SlidingWindowCounterRateLimiter,
            storage_from_string(uri, **args),
            limit,
        )
    )

import asyncio

from . import camonitor, CANothing


class PVCache:
    """Monitor a list of PVs with access to the most recent value.

    Args:
        pvs: A list of PV names, or a dict where the values are PV names.
        notify_disconnect: When False (default) omits notifications if some PVs are disconnected

    When the pvs argument is a list, then get() and friends will return a list of values in the same order.
    When pvs is a dict, then get() and friends will return a dict preserving the keys.

    As a convenience, a PVCache may be (asynchronously) iterated to yield new values.  eg. in list mode. ::

        await for val1, val2 in PVCache(['pv1', 'pv2']):
            print(val1, '+', val2, '=', val1+val2)

    Or in dict mode. ::

        await for V in PVCache({'A':'pv1', 'B':'pv2'}):
            print(V['A'], '+', V['B'], '=', V['A']+V['B'])

    A cache may be polled for the current values with get() and get_nowait().
    The changed() coroutine completes after the cache has been updated. ::

        cache = PVCache(['pv1', 'pv2']):
        while True:
            val1, val2 = await cache.get()
            print(val1, '+', val2, '=', val1+val2)
            await cache.changed()

    """

    def __init__(self, pvs, notify_disconnect=False):
        self._notify_disconnect = notify_disconnect

        if isinstance(pvs, dict):
            self._keys = pvs.keys()
            self._pvs = pvs.values()
        else:
            self._keys = None
            self._pvs = list(pvs)

        self._cache = [None] * len(self._pvs)

        self._updated = asyncio.Event()

        self._subs = None
        self._subs = camonitor(self._pvs, self._update_cache, notify_disconnect=True)

    def close(self):
        """End subscriptions and stop updating cache.
        """
        if self._subs is not None:
            subs, self._subs = self._subs, None
            for sub in subs:
                sub.close()

    async def get(self):
        """Complete with current values from cache.
        """
        while not self._notify_disconnect and not self._all_connected():
            await self._updated.wait()

        return self.get_nowait()

    __getitem__ = get

    async def changes(self):
        """Complete when some values in the cache have changed.
        """
        while True:
            await self._updated.wait()
            if self._notify_disconnect or self._all_connected():
                break

    def get_nowait(self):
        """Fetch current entries from cache immediately.  Returns None/[None] when disconnected.
        """
        if self._keys is None:
            return list(self._cache)  # shallow copy
        else:
            return dict(zip(self._keys, self._cache))

    def _update_cache(self, val, i):
        if isinstance(val, CANothing):
            val = None

        self._cache[i] = val

        # asyncio.Event is not self-resetting.
        # Since we won't be preempted, we can "pulse" to achieve the same effect
        self._updated.set()
        self._updated.clear()

    def _all_connected(self):
        for ent in self._cache:
            if ent is None:
                return False
        return True

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self,A,B,C):
        self.close()

    def __aiter__(self):
        """PVCache is iterable to yield snapshots as returned by get_nowait()
        """
        return self.Iter(self)

    class Iter:
        def __init__(self, cache):
            self._cache, self._first = cache, True

        async def __anext__(self):
            while True:
                if self._first:
                    # (maybe) deliver initial snapshot immediately
                    self._first = False
                else:
                    await self._cache.changed()

            return self._cache.get_nowait()

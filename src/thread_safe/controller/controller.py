import asyncio
import threading

# Equivalent to the Go 'Controller' struct and its methods
class Controller:
    def __init__(self, interval: float, start_now: bool):
        # Interval is stored in seconds (float)
        self.interval = interval
        self._timer = None
        # Use an Event object to signal when a 'tick' has occurred
        self._tick_event = threading.Event()
        self._running = True
        self._setup_timer(1 if start_now else interval)


    def _setup_timer(self, current_interval):
        # Stop the old timer if it exists
        if self._timer:
            self._timer.cancel()

        # Start a new timer that runs self._on_tick() after current_interval seconds
        self._timer = threading.Timer(current_interval, self._on_tick)
        self._timer.daemon = True  # Allows program to exit if main thread closes
        self._timer.start()

    def _on_tick(self):
        """Called when the timer expires. Sets the event and resets the timer."""
        if self._running:
            self._tick_event.set()  # Signal that a tick happened
            self._tick_event.clear()
            self._setup_timer(self.interval)

    def close(self):
        """Stops the internal timer."""
        self._running = False
        if self._timer:
            self._timer.cancel()
        # Ensure the event is set so any waiting loops can exit
        self._tick_event.set()

    def wait(self):
        return self._tick_event.wait()

    def ticker(self) -> threading.Event:
        """Returns the event object to wait on for ticks."""
        return self._tick_event

    def clear(self):
        """Clears the internal timer."""
        self._tick_event.clear()

class AsyncController:
    def __init__(self, interval: float, start_now: bool):
        self.interval = interval
        self._tick_event = asyncio.Event()
        self.running = True
        self._task = None
        self._sleep_task = None
        self._start_now = start_now

    def trigger(self):
        """Manually trigger the event to bypass the current sleep interval."""
        self._tick_event.set()

    async def _run_ticker(self, initial_delay):
        """Internal loop that acts like the threading.Timer."""
        try:
            if initial_delay > 0:
                try:
                    self._sleep_task = asyncio.create_task(asyncio.sleep(initial_delay))
                    await self._sleep_task
                except asyncio.CancelledError:
                    pass

            while self.running:
                self._tick_event.set()
                # Crucial: asyncio.Event.set() doesn't auto-clear like some Go channels.
                # We clear immediately so the NEXT wait() actually blocks.
                self._tick_event.clear()
                try:
                    self._sleep_task = asyncio.create_task(asyncio.sleep(self.interval))
                    await self._sleep_task
                except asyncio.CancelledError:
                    pass
        except asyncio.CancelledError:
            pass

    async def wait(self):
        """Blocks until the interval passes OR trigger() is called."""
        if not self.running:
            return

        # If it's the very first run and start_now is True, skip the sleep
        if self._start_now:
            self._start_now = False
            return

        try:
            # Wait for either the interval to finish OR a manual trigger event
            await asyncio.wait_for(self._tick_event.wait(), timeout=self.interval)
        except asyncio.TimeoutError:
            # Timeout means the interval naturally completed!
            pass
        finally:
            # Clear the manual trigger so it resets for the next loop iteration
            self._tick_event.clear()

    def ticker(self) -> asyncio.Event:
        return self._tick_event

    def close(self):
        """Stops the ticker and cleans up the background task."""
        self.running = False
        self._tick_event.set()

    def clear(self):
        self._tick_event.clear()
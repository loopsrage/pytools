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

        # Start the background ticking task
        # We use create_task so it runs concurrently with your other code
        initial_delay = 0 if start_now else self.interval
        self._task = asyncio.create_task(self._run_ticker(initial_delay))

    def trigger(self):
        """Manually trigger the event to bypass the current sleep interval."""
        self._tick_event.set()

    async def _run_ticker(self, initial_delay):
        """Internal loop that acts like the threading.Timer."""
        try:
            if initial_delay > 0:
                await asyncio.sleep(initial_delay)

            while self.running:
                self._tick_event.set()
                # Crucial: asyncio.Event.set() doesn't auto-clear like some Go channels.
                # We clear immediately so the NEXT wait() actually blocks.
                self._tick_event.clear()

                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            pass

    async def wait(self):
        """Async equivalent to self._tick_event.wait()."""
        await self._tick_event.wait()

    def ticker(self) -> asyncio.Event:
        return self._tick_event

    def close(self):
        """Stops the ticker and cleans up the background task."""
        self.running = False
        if self._task:
            self._task.cancel()
        self._tick_event.set()

    def clear(self):
        self._tick_event.clear()
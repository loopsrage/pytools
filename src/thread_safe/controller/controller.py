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


from typing import Callable, Awaitable
import asyncio
from logger import logger


class AsyncTimer:
    def __init__(self, get_delay: Callable[[], float], callback: Callable[[], Awaitable[None]], timer_name: str):
        """
        Initialize the timer.
        :param delay: The delay in seconds before the callback is executed.
        :param callback: The async function to be called after the delay.
        """
        self._get_delay = get_delay
        self._callback = callback
        self._timer_name = timer_name
        self._task = None

    async def _run(self):
        """Private method to wait for the delay and call the callback."""
        try:
            delay = self._get_delay()
            logger.debug(f'timer {self._timer_name} set to {delay} seconds')
            await asyncio.sleep(delay)  # Wait for the delay
            await self._callback()  # Call the async callback
        except asyncio.CancelledError:
            # Handle timer cancellation
            pass

    def start(self):
        """Start the timer."""
        if self._task is None:  # Avoid restarting if already running
            self._task = asyncio.create_task(self._run())

    def cancel(self):
        """Cancel the timer."""
        if self._task:
            self._task.cancel()
            self._task = None
        logger.debug(f'timer {self._timer_name} cancelled')

    def restart(self):
        """Restart the timer."""
        self.cancel()
        self.start()

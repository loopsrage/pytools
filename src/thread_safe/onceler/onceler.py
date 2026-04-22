import asyncio
import threading
from typing import Any, Callable

from thread_safe.index import Index


class Onceler:
    def __init__(self):
        self.index_manager = Index()
        # Initialize our indexes
        self.index_manager.new("results")
        self.index_manager.new("locks")

    async def astore_once(self, index_name: str, key: Any, do: Callable[[], Any]):
        full_key = f"{index_name}:{key}"
        existing_result = self.index_manager.load_from_index("results", full_key)
        if existing_result is not None:
            return self._handle_result(existing_result)

        actual_lock, _ = self.index_manager.load_or_store_in_index(
            index_name="locks",
            key=full_key,
            value=asyncio.Lock()
        )
        async with actual_lock:
            existing_result = self.index_manager.load_from_index("results", full_key)
            if existing_result is not None:
                return self._handle_result(existing_result)

            try:
                result = await do()
                stored_value = result if result is not None else "COMPLETED"
                self.index_manager.store_in_index("results", full_key, stored_value)
                return result
            except Exception as e:
                self.index_manager.store_in_index("results", full_key, e)
                raise e
            finally:
                self.index_manager.delete_from_index("locks", full_key)

    def store_once(self, index_name: str, key: Any, do: Callable[[], Any]) -> Any:
        """
        Ensures the 'do' function runs only once per index/key pair.
        Uses ConcurrentIndex for lock-free state checks.
        """
        full_key = f"{index_name}:{key}"

        existing_result = self.index_manager.load_from_index("results", full_key)
        if existing_result is not None:
            return self._handle_result(existing_result)

        # load_or_store_in_index uses put_if_absent internally
        actual_lock, _ = self.index_manager.load_or_store_in_index(
            index_name="locks",
            key=full_key,
            value=threading.Lock()
        )

        with actual_lock:
            existing_result = self.index_manager.load_from_index("results", full_key)
            if existing_result is not None:
                return self._handle_result(existing_result)

            try:
                result = do()
                # Store the successful result
                stored_value = result if result is not None else "COMPLETED"
                self.index_manager.store_in_index("results", full_key, stored_value)
                return result
            except Exception as e:
                self.index_manager.store_in_index("results", full_key, e)
                raise e
            finally:
                self.index_manager.delete_from_index("locks", full_key)

    def _handle_result(self, value: Any) -> Any:
        if isinstance(value, Exception):
            raise value
        return value if value != "COMPLETED" else None
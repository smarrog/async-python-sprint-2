from functools import wraps
from threading import Lock
from typing import Callable


def coroutine(f):
    @wraps(f)  # https://docs.python.org/3/library/functools.html#functools.wraps
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen

    return wrap


# https://pypi.org/project/cancel-token/


class CancellationToken:
    def __init__(self) -> None:
        self._callbacks: list[Callable[[], None]] = []
        self._is_canceled = False
        self._is_completed = False
        self._lock = Lock()

    def on_cancel(self, callback: Callable[[], None]) -> None:
        if not self._is_canceled:
            with self._lock:
                if not self._is_canceled:
                    self._callbacks.append(callback)
                    return

        callback()

    def remove_callback(self, callback: Callable[[], None]) -> None:
        self._callbacks.remove(callback)

    def cancel(self) -> None:
        with self._lock:
            if self._is_canceled:
                return

            self._is_canceled = True
            self._is_completed = True

        for f in [x for x in self._callbacks]:
            f()

    def complete(self) -> None:
        with self._lock:
            self._is_completed = True

    @property
    def is_cancelled(self) -> bool:
        return self._is_canceled

    @property
    def is_completed(self) -> bool:
        return self._is_completed

    @property
    def is_active(self) -> bool:
        return not self._is_canceled and not self._is_completed


import datetime as dt
import os
import shutil
import logging
from pathlib import Path
import urllib.request
import urllib.parse
import time
from typing import Self, Any, Callable, Generator, Optional
from enum import Enum
from uuid import uuid4, UUID
from threading import Thread
from abc import ABC, abstractmethod

from exceptions import IncorrectJobStateException
from utils import coroutine, CancellationToken

TIMEOUT_ERROR = "Timeout"
NO_TRIES_LEFT_ERROR = "No tries left"
MANUALLY_FAILED_ERROR = "Manually failed"

logger = logging.getLogger()


class Job(ABC):
    CompleteHandler = Callable[[Self], None]

    class State(Enum):
        PENDING = 1
        RUNNING = 2
        COMPLETED = 3
        FAILED = 4

    def __init__(
            self,
            start_at: dt = dt.datetime.now(),
            max_working_time: float = 0,
            tries: int = 1,
            dependencies: Optional[list[UUID]] = None
    ):
        self._id = uuid4()
        self._start_at = start_at
        self._max_working_time = max_working_time
        self._tries = tries
        self._dependencies = dependencies
        self._result: Any = None
        self._state: Job.State = Job.State.PENDING
        self._coroutine: Optional[Generator] = None
        self._timeout_cancellation_token: Optional[CancellationToken] = None
        self._on_complete_handlers: set[Job.CompleteHandler] = set()

    def __repr__(self) -> str:
        return f"<Job [{self.__class__}] \"{self._id}\" ({self._state.name}) -> {self._result}>"

    @property
    def id(self) -> UUID:
        return self._id

    @property
    def start_at(self) -> dt:
        return self._start_at

    @property
    def can_be_started(self) -> bool:
        return self._tries > 0

    @property
    def result(self) -> Any:
        return self._result

    @property
    def state(self) -> State:
        return self._state

    @property
    def dependencies(self) -> Optional[list[UUID]]:
        return self._dependencies

    def run(self) -> None:
        if self._state != Job.State.PENDING:
            raise IncorrectJobStateException()

        try:
            timeout_cancellation_token = CancellationToken()
            self._timeout_cancellation_token = timeout_cancellation_token

            self._set_state(Job.State.RUNNING)
            self._result = None
            self._coroutine = self.__start_coroutine()

            if not self.can_be_started:
                self._notify_error(NO_TRIES_LEFT_ERROR)
                return

            logger.info("Run %s", self)
            self._run()

            self.__start_timeout(timeout_cancellation_token)
        except (Exception,):
            self._notify_error("Internal job error")

    def make_failed(self) -> None:
        if self._state != Job.State.PENDING:
            raise IncorrectJobStateException()

        self._result = MANUALLY_FAILED_ERROR
        self._set_state(Job.State.FAILED)

        logger.info("Manually failed %s", self)

        self.__notify_all_subscribers()

    def add_complete_handler(self, handler: CompleteHandler):
        # noinspection PyTypeChecker
        self._on_complete_handlers.add(handler)

    def remove_complete_handler(self, handler: CompleteHandler):
        # noinspection PyTypeChecker
        self._on_complete_handlers.remove(handler)

    def remove_all_complete_handlers(self):
        self._on_complete_handlers.clear()

    def stop(self) -> None:
        if self._state is not Job.State.RUNNING:
            raise IncorrectJobStateException()

        logger.info("Stop %s", self)

        self.__stop_timeout()
        self._result = None
        self._set_state(Job.State.PENDING)

    def restart(self) -> None:
        if self._state is not Job.State.COMPLETED and self._state is not Job.State.FAILED:
            raise IncorrectJobStateException()

        logger.info("Restart %s", self)

        self._result = None
        self._set_state(Job.State.PENDING)

        self.run()

    @abstractmethod
    def _run(self) -> None:
        pass

    def _set_state(self, state: State):
        self._state = state

    def _notify_complete(self, result: Any) -> None:
        if self._state is not Job.State.RUNNING:
            raise IncorrectJobStateException()

        self.__stop_timeout()

        self._result = self._coroutine.send(result)
        self._set_state(Job.State.COMPLETED)

        logger.info("Complete %s", self)

        self.__notify_all_subscribers()

    def _notify_error(self, error: Any) -> None:
        if self._state is not Job.State.RUNNING:
            raise IncorrectJobStateException()

        self.__stop_timeout()

        self._tries = self._tries - 1
        self._result = self._coroutine.send(error)
        self._set_state(Job.State.FAILED)

        logger.info("Fail %s", self)

        self.__notify_all_subscribers()

    @coroutine
    def __start_coroutine(self) -> Any:
        result = (yield)
        yield result

    def __start_timeout(self, cancellation_token: CancellationToken) -> None:
        if self._max_working_time <= 0 or not cancellation_token.is_active:
            return

        def task() -> None:
            time.sleep(self._max_working_time)
            if cancellation_token.is_active:
                self._notify_error(TIMEOUT_ERROR)

        thread = Thread(target=task)
        thread.start()

    def __stop_timeout(self) -> None:
        if self._timeout_cancellation_token is None:
            return

        if self._timeout_cancellation_token.is_active:
            self._timeout_cancellation_token.cancel()  # always cancel for simplicity
        self._timeout_cancellation_token = None

    def __notify_all_subscribers(self) -> None:
        copy: set[Job.CompleteHandler] = set(self._on_complete_handlers)
        self._on_complete_handlers.clear()
        for handler in copy:
            handler(self)


class SimpleSyncJob(Job):
    def __init__(self,
                 worker: Callable[[], Any],
                 start_at: dt = dt.datetime.now(),
                 max_working_time: float = 0,
                 tries: int = 1,
                 dependencies: list | None = None):
        self._worker = worker
        super().__init__(start_at, max_working_time, tries, dependencies)

    def _run(self) -> None:
        result = self._worker()
        self._notify_complete(result)


class SimpleAsyncJob(SimpleSyncJob):
    def __init__(self,
                 worker: Callable[[], Any],
                 delay: float,
                 start_at: dt = dt.datetime.now(),
                 max_working_time: float = 0,
                 tries: int = 1,
                 dependencies: list | None = None):
        self._delay = delay
        self._task_cancellation_token: CancellationToken | None = None
        super().__init__(worker, start_at, max_working_time, tries, dependencies)

    def _run(self) -> None:
        task_cancellation_token = CancellationToken()
        self._task_cancellation_token = task_cancellation_token

        def task(cancellation_token: CancellationToken) -> None:
            time.sleep(self._delay)
            if cancellation_token.is_active:
                SimpleSyncJob._run(self)

        thread = Thread(target=task, args=[task_cancellation_token])
        thread.start()

    def _set_state(self, state: Job.State) -> None:
        self.__stop_waiting_for_run()
        Job._set_state(self, state)

    def __stop_waiting_for_run(self) -> None:
        if self._task_cancellation_token is None:
            return

        if self._task_cancellation_token.is_active:
            self._task_cancellation_token.cancel()  # always cancel for simplicity
        self._task_cancellation_token = None


class CreateDirectoryJob(Job):
    def __init__(self,
                 path: str,
                 start_at: dt = dt.datetime.now(),
                 max_working_time: float = 0,
                 tries: int = 1,
                 dependencies: list | None = None):
        self._path = path
        super().__init__(start_at, max_working_time, tries, dependencies)

    def _run(self) -> None:
        Path(self._path).mkdir(parents=True, exist_ok=True)

        logger.info("Directory %s was created", self._path)

        self._notify_complete(True)


class RemoveDirectoryJob(Job):
    def __init__(self,
                 path: str,
                 is_recursive: bool = False,
                 start_at: dt = dt.datetime.now(),
                 max_working_time: float = 0,
                 tries: int = 1,
                 dependencies: list | None = None):
        self._path = path
        self._is_recursive = is_recursive
        super().__init__(start_at, max_working_time, tries, dependencies)

    def _run(self) -> None:
        if os.path.isdir(self._path):
            os.rmdir(self._path)
        else:
            shutil.rmtree(self._path)

        logger.info("Directory %s was removed", self._path)

        self._notify_complete(True)


class WriteTextFileJob(Job):
    def __init__(self,
                 path: str,
                 text: str,
                 is_append: bool = False,
                 start_at: dt = dt.datetime.now(),
                 max_working_time: float = 0,
                 tries: int = 1,
                 dependencies: list | None = None):
        self._path = path
        self._text = text
        self._is_append = is_append
        super().__init__(start_at, max_working_time, tries, dependencies)

    def _run(self) -> None:
        if self._is_append:
            mode = "a"
        else:
            mode = "w"

        with open(self._path, mode) as f:
            f.write(self._text)

        logger.info("File %s was updated", self._path)

        self._notify_complete(True)


class ReadTextFileJob(Job):
    def __init__(self,
                 path: str,
                 start_at: dt = dt.datetime.now(),
                 max_working_time: float = 0,
                 tries: int = 1,
                 dependencies: list | None = None):
        self._path = path
        super().__init__(start_at, max_working_time, tries, dependencies)

    def _run(self) -> None:
        with open(self._path, "r") as f:
            result = f.read()

        logger.info("%s contains: %s", self._path, result)

        self._notify_complete(result)


class RemoveFileJob(Job):
    def __init__(self,
                 path: str,
                 start_at: dt = dt.datetime.now(),
                 max_working_time: float = 0,
                 tries: int = 1,
                 dependencies: list | None = None):
        self._path = path
        super().__init__(start_at, max_working_time, tries, dependencies)

    def _run(self) -> None:
        if os.path.isfile(self._path):
            os.remove(self._path)

        logger.info("File %s was removed", self._path)

        self._notify_complete(True)


class UrlRequestJob(Job):
    def __init__(self,
                 url: str,
                 start_at: dt = dt.datetime.now(),
                 max_working_time: float = 0,
                 tries: int = 1,
                 dependencies: list | None = None):
        self._url = url
        super().__init__(start_at, max_working_time, tries, dependencies)

    def _run(self) -> None:
        response = urllib.request.urlopen(self._url)
        result = response.read().decode('utf-8')

        logger.info("Request to %s completed and parsed", self._url)

        self._notify_complete(result)

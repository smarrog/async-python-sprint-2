import logging
import threading
import time
from copy import copy
from datetime import datetime as dt
from typing import Generator, Optional
from uuid import UUID

from exceptions import PoolSizeException, IncorrectJobStateException, JobTwiceSchedulingException
from job import Job
from utils import CancellationToken

logger = logging.getLogger()


class Scheduler:
    def __init__(self, pool_size: int = 10):
        self._pool_size = pool_size
        self._pending: set[Job] = set()
        self._running: set[Job] = set()
        self._completed: set[UUID] = set()
        self._failed: set[UUID] = set()
        self._coroutine: Generator | None = None
        self._check_time: Optional[dt] = None
        self._cancellation_token: Optional[CancellationToken] = None

    @property
    def total_jobs_amount(self) -> int:
        return len(self._pending) + len(self._running)

    @property
    def is_running(self) -> bool:
        return self._cancellation_token is not None

    def schedule(self, job) -> None:
        if self.total_jobs_amount >= self._pool_size:
            raise PoolSizeException()

        if job in self._pending or job in self._running or job.id in self._completed or job.id in self._failed:
            raise JobTwiceSchedulingException()

        logger.info("Schedule %s", job)

        self._pending.add(job)

        if job.dependencies is not None:
            for dependency in job.dependencies:
                if dependency in self._failed:
                    job.make_failed()
                    self.__on_job_failed(job)
                    return

        self.__start_job_if_can(job)

    def run(self) -> None:
        logger.info("Run scheduler")

        self._cancellation_token = CancellationToken()

        pending_copy = copy(self._pending)
        for job in pending_copy:
            if job in self._pending:  # can be already executed if there were maby tasks in a dependency row
                self.__start_job_if_can(job)

    def stop(self) -> None:
        logger.info("Stop scheduler")

        self._cancellation_token.cancel()
        self._cancellation_token = None

        running_copy = copy(self._running)
        self._running.clear()
        for job in running_copy:
            job.remove_all_complete_handlers()  # just for simplicity remove all
            job.stop()
            self._pending.add(job)

    def __auto_fail_jobs_with_dependency(self, job_id: UUID) -> None:
        marked_as_failed = []  # we need it due to recursion to prevent changing iterator

        for job in self._pending:
            if job.dependencies is not None and job_id in job.dependencies and job.state is not Job.State.FAILED:
                # if state is FAILED already it means that it come from recursion
                job.make_failed()
                marked_as_failed.append(job)

        # and now we need to check recursion for failed jobs
        for failed_job in marked_as_failed:
            self.__on_job_failed(failed_job)

    def __start_job_if_can(self, job: Job) -> bool:
        if not self.is_running:
            return False

        logger.info("Check %s", job)

        if not self.__is_job_available_by_dependencies(job):
            return False  # it will be checked again when dependencies are completed

        if not self.__is_job_available_by_time(job):
            self.__schedule_job_start(job, self._cancellation_token)
            return False

        self.__start_job(job)

        return True

    def __start_job(self, job: Job) -> None:
        def on_job_complete(completed_job: Job):
            if completed_job.state is Job.State.COMPLETED:
                self.__on_job_completed_successfully(completed_job)
            elif completed_job.state is Job.State.FAILED:
                self.__on_job_failed(completed_job)
            else:
                raise IncorrectJobStateException()

        job.add_complete_handler(on_job_complete)
        self._pending.remove(job)
        self._running.add(job)
        job.run()

    def __schedule_job_start(self, job: Job, cancellation_token: CancellationToken) -> None:
        def task():
            if not cancellation_token.is_active:
                return

            if not self.is_running or job not in self._pending:  # it somehow can be removed from pending
                return

            self.__start_job(job)

        thread = threading.Timer(
            interval=job.start_at.timestamp() - time.time(),
            function=task)
        thread.start()

    @staticmethod
    def __is_job_available_by_time(job: Job) -> bool:
        return job.start_at <= dt.now()

    def __is_job_available_by_dependencies(self, job: Job) -> bool:
        if job.dependencies is not None:
            for dependency in job.dependencies:
                if dependency not in self._completed:
                    return False

        return True

    def __on_job_completed_successfully(self, job: Job) -> None:
        self._running.remove(job)
        self._completed.add(job.id)

        pending_copy = copy(self._pending)
        for pending_job in pending_copy:
            if pending_job.dependencies is not None and job.id in pending_job.dependencies:
                self.__start_job_if_can(pending_job)

    def __on_job_failed(self, job: Job) -> None:
        is_auto_fail = job in self._pending  # if job is in pending we came here from auto fail by dependencies
        if job.can_be_started and not is_auto_fail:
            job.restart()
            return

        if is_auto_fail:
            self._pending.remove(job)
        else:
            self._running.remove(job)

        logger.info("Job %s added to failed", job.id)
        self._failed.add(job.id)

        self.__auto_fail_jobs_with_dependency(job.id)

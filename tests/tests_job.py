import unittest

from job import SimpleSyncJob, SimpleAsyncJob, Job
from exceptions import IncorrectJobStateException


class JobTestCase(unittest.TestCase):
    def test_that_ids_are_unique(self) -> None:
        job_1 = self.__get_simple_job()
        job_2 = self.__get_simple_job()
        job_3 = self.__get_simple_job()

        self.assertNotEqual(job_1.id, job_2.id, job_3.id)

    def test_can_be_started_on_default_tries(self) -> None:
        job = self.__get_simple_job()

        self.assertTrue(job.can_be_started)

    def test_can_be_started_when_enough_tries(self) -> None:
        job = SimpleSyncJob(self.__simple_worker, tries=1)

        self.assertTrue(job.can_be_started)

    def test_can_be_started_when_not_enough_tries(self) -> None:
        job = SimpleSyncJob(self.__simple_worker, tries=0)

        self.assertFalse(job.can_be_started)

    def test_that_start_state_is_pending(self) -> None:
        job = self.__get_simple_job()

        self.assertEqual(Job.State.PENDING, job.state)

    def test_that_complete_handler_is_called_after_success(self) -> None:
        flag = False

        def complete_handler(job: Job) -> None:
            nonlocal flag

            flag = True

        job = self.__get_simple_job()
        job.add_complete_handler(complete_handler)
        job.run()

        self.assertEqual(Job.State.COMPLETED, job.state)
        self.assertTrue(flag)

    def test_that_complete_handler_is_called_after_fail(self) -> None:
        flag = False

        def complete_handler(job: Job) -> None:
            nonlocal flag

            flag = True

        job = self.__get_simple_auto_failed_job()
        job.add_complete_handler(complete_handler)
        job.run()

        self.assertEqual(Job.State.FAILED, job.state)
        self.assertTrue(flag)

    def test_that_complete_handler_is_not_called_twice(self):
        value = 0

        def complete_handler(completed_job: Job) -> None:
            nonlocal value

            value = value + 1

        job = self.__get_simple_job()
        job.add_complete_handler(complete_handler)
        job.run()

        job.restart()

        self.assertEqual(1, value)

    def test_that_complete_handler_cannot_be_added_twice(self):
        value = 0

        def complete_handler(completed_job: Job) -> None:
            nonlocal value

            value = value + 1

        job = self.__get_simple_job()
        job.add_complete_handler(complete_handler)
        job.add_complete_handler(complete_handler)
        job.run()

        self.assertEqual(1, value)

    def test_that_complete_handler_is_noe_called_after_remove(self) -> None:
        flag = False

        def complete_handler(job: Job) -> None:
            nonlocal flag

            flag = True

        job = self.__get_simple_job()
        job.add_complete_handler(complete_handler)
        job.remove_complete_handler(complete_handler)
        job.run()

        self.assertFalse(flag)

    def test_that_stop_in_pending_state_leeds_to_exception(self) -> None:
        job = self.__get_simple_job()

        self.assertEqual(Job.State.PENDING, job.state)

        with self.assertRaises(IncorrectJobStateException):
            job.stop()

    def test_that_stop_in_complete_state_leeds_to_exception(self) -> None:
        job = self.__get_simple_job()
        job.run()

        self.assertEqual(Job.State.COMPLETED, job.state)

        with self.assertRaises(IncorrectJobStateException):
            job.stop()

    def test_that_stop_in_failed_state_leeds_to_exception(self) -> None:
        job = self.__get_simple_auto_failed_job()
        job.run()

        self.assertEqual(Job.State.FAILED, job.state)

        with self.assertRaises(IncorrectJobStateException):
            job.stop()

    def test_that_stop_in_running_state_stops_the_task(self) -> None:
        job = self.__get_simple_async_job()
        job.run()

        self.assertEqual(Job.State.RUNNING, job.state)

        job.stop()

        self.assertEqual(Job.State.PENDING, job.state)

    def test_that_restart_in_pending_state_leeds_to_exception(self) -> None:
        job = self.__get_simple_job()

        self.assertEqual(Job.State.PENDING, job.state)

        with self.assertRaises(IncorrectJobStateException):
            job.restart()

    def test_that_stop_in_running_state_leeds_to_exception(self) -> None:
        job = self.__get_simple_async_job()
        job.run()

        self.assertEqual(Job.State.RUNNING, job.state)

        job.stop()

        with self.assertRaises(IncorrectJobStateException):
            job.restart()

    def test_that_restart_in_complete_state_restarts_the_task(self) -> None:
        value: int = 0

        def worker() -> None:
            nonlocal value

            value = value + 1

        job = SimpleSyncJob(worker)
        job.run()

        self.assertEqual(Job.State.COMPLETED, job.state)

        job.restart()

        self.assertEqual(Job.State.COMPLETED, job.state)
        self.assertEqual(2, value)

    def test_that_restart_in_failed_state_restarts_the_task(self) -> None:
        value: int = 0

        def worker() -> None:
            nonlocal value

            value = value + 1
            if value == 1:
                raise Exception()

        job = SimpleSyncJob(worker)
        job.run()

        self.assertEqual(Job.State.FAILED, job.state)

        job.restart()

        self.assertEqual(Job.State.FAILED, job.state)

    def test_that_tries_decreases_after_fail(self) -> None:
        job = SimpleSyncJob(JobTestCase.__simple_worker, tries=2)
        job.run()

        self.assertEqual(2, job._tries)

    def test_that_tries_do_not_decreases_after_complete(self) -> None:
        job = self.__get_simple_auto_failed_job()
        job.run()

        self.assertEqual(0, job._tries)

    @staticmethod
    def __get_simple_async_job(delay: float = 0.1) -> SimpleSyncJob:
        return SimpleAsyncJob(JobTestCase.__simple_worker, delay=delay)

    @staticmethod
    def __get_simple_job() -> SimpleSyncJob:
        return SimpleSyncJob(JobTestCase.__simple_worker)

    @staticmethod
    def __get_simple_auto_failed_job() -> SimpleSyncJob:
        def worker() -> bool:
            raise Exception()

        return SimpleSyncJob(worker)

    @staticmethod
    def __simple_worker() -> bool:
        return True

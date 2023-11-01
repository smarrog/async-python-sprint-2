import unittest

from job import SimpleSyncJob, SimpleAsyncJob, Job
from scheduler import Scheduler
from exceptions import PoolSizeException, JobTwiceSchedulingException


class SchedulerTestCase(unittest.TestCase):
    def test_scheduler_size(self):
        s = Scheduler()
        s.schedule(self.__get_simple_job())
        s.schedule(self.__get_simple_job())
        s.schedule(self.__get_simple_async_job())

        self.assertEqual(3, s.total_jobs_amount)

        s.run()

        self.assertEqual(1, s.total_jobs_amount)

    def test_scheduler_oversize(self):
        s = Scheduler(2)
        s.schedule(self.__get_simple_job())
        s.schedule(self.__get_simple_job())

        with self.assertRaises(PoolSizeException):
            s.schedule(self.__get_simple_job())

    def test_is_running(self):
        s = Scheduler()

        self.assertFalse(s.is_running)

        s.run()

        self.assertTrue(s.is_running)

        s.stop()

        self.assertFalse(s.is_running)

    def test_schedule_when_not_running(self):
        s = Scheduler()
        job = self.__get_simple_job()
        s.schedule(job)

        self.assertEqual(Job.State.PENDING, job.state)

    def test_schedule_when_running(self):
        s = Scheduler()
        s.run()
        job = self.__get_simple_job()
        s.schedule(job)

        self.assertEqual(Job.State.COMPLETED, job.state)

    def test_schedule_twice(self):
        s = Scheduler()
        job = self.__get_simple_job()
        s.schedule(job)

        with self.assertRaises(JobTwiceSchedulingException):
            s.schedule(job)

    def test_schedule_twice_when_in_competed(self):
        s = Scheduler()
        s.run()
        job = self.__get_simple_job()
        s.schedule(job)

        with self.assertRaises(JobTwiceSchedulingException):
            s.schedule(job)

    def test_schedule_when_dependency_in_failed(self):
        s = Scheduler()
        s.run()

        failed_job = self.__get_simple_auto_failed_job()
        job_with_dependency = SimpleSyncJob(SchedulerTestCase.__simple_worker, dependencies=[failed_job.id])

        s.schedule(failed_job)
        s.schedule(job_with_dependency)

        self.assertEqual(Job.State.FAILED, job_with_dependency.state)

    def test_schedule_when_dependency_is_not_completed(self):
        s = Scheduler()
        s.run()

        async_job = self.__get_simple_async_job()
        job_with_dependency = SimpleSyncJob(SchedulerTestCase.__simple_worker, dependencies=[async_job.id])

        s.schedule(async_job)
        s.schedule(job_with_dependency)

        self.assertEqual(Job.State.PENDING, job_with_dependency.state)

    def test_schedule_when_dependency_is_completed(self):
        s = Scheduler()
        s.run()

        simple_job = self.__get_simple_job()
        job_with_dependency = SimpleSyncJob(SchedulerTestCase.__simple_worker, dependencies=[simple_job.id])

        s.schedule(simple_job)
        s.schedule(job_with_dependency)

        self.assertEqual(Job.State.COMPLETED, job_with_dependency.state)

    @staticmethod
    def __get_simple_async_job(delay: float = 0.1) -> SimpleSyncJob:
        return SimpleAsyncJob(SchedulerTestCase.__simple_worker, delay=delay)

    @staticmethod
    def __get_simple_job() -> SimpleSyncJob:
        return SimpleSyncJob(SchedulerTestCase.__simple_worker)

    @staticmethod
    def __get_simple_auto_failed_job() -> SimpleSyncJob:
        def worker() -> bool:
            raise Exception()

        return SimpleSyncJob(worker)

    @staticmethod
    def __simple_worker() -> bool:
        return True

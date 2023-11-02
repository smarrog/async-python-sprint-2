import os.path
import time
import datetime as dt
import logging

from job import (
    Job,
    SimpleSyncJob,
    SimpleAsyncJob,
    CreateDirectoryJob,
    WriteTextFileJob,
    ReadTextFileJob,
    RemoveFileJob,
    RemoveDirectoryJob,
    UrlRequestJob
)
from scheduler import Scheduler
from log_settings import init as init_logging

logger = logging.getLogger()


def empty_worker():
    pass


def job_worker():
    return "Sync job result"


def async_job_worker():
    return "Async job result"


def bad_job_worker():
    raise Exception()


def async_job_complete_handler(completed_job: Job):
    logger.info("Call complete handler for async job %s", completed_job)
    completed_job.restart()  # no additional complete handler here


def naked_jobs_scenario():
    logging.debug("======> Naked jobs")

    async_job = SimpleAsyncJob(async_job_worker, delay=0.2, max_working_time=0.3, tries=2)
    async_job.add_complete_handler(async_job_complete_handler)
    async_job.run()

    sync_job = SimpleSyncJob(job_worker, tries=1)
    sync_job.run()

    bad_job = SimpleSyncJob(bad_job_worker, tries=2)
    bad_job.run()
    bad_job.restart()
    bad_job.restart()

    time.sleep(0.1)
    async_job.stop()

    async_job.run()

    time.sleep(1)


def scheduler_scenario():
    logging.debug("======> Scheduler")

    s = Scheduler()

    job_1 = SimpleSyncJob(empty_worker)
    job_2 = SimpleSyncJob(bad_job_worker)
    job_3 = SimpleAsyncJob(empty_worker, delay=0.2)
    job_4 = SimpleAsyncJob(empty_worker, delay=0.2, dependencies=[job_2.id])
    job_5 = SimpleAsyncJob(empty_worker, delay=0.2, dependencies=[job_4.id])
    job_6 = SimpleSyncJob(empty_worker, dependencies=[job_1.id, job_3.id])
    job_7 = SimpleSyncJob(bad_job_worker)
    job_8 = SimpleSyncJob(empty_worker, start_at=dt.datetime.now() + dt.timedelta(seconds=0.2))

    s.schedule(job_1)
    s.schedule(job_2)
    s.schedule(job_3)
    s.schedule(job_4)
    s.schedule(job_5)
    s.schedule(job_8)

    s.run()

    s.schedule(job_6)
    s.schedule(job_7)

    time.sleep(0.1)

    s.stop()
    s.run()

    time.sleep(2)


def files_scenario():
    logging.debug("======> Files system")

    s = Scheduler()

    directory_path = os.path.join("output", "files")
    file_name = "tmp.txt"
    file_path = os.path.join(directory_path, file_name)

    create_directory_job = CreateDirectoryJob(directory_path)
    create_file_job = WriteTextFileJob(file_path, "Hello", dependencies=[create_directory_job.id])
    append_file_job = WriteTextFileJob(file_path, " World", True, dependencies=[create_file_job.id])
    read_file_job = ReadTextFileJob(file_path, dependencies=[append_file_job.id])
    remove_file_job = RemoveFileJob(file_path, dependencies=[read_file_job.id])
    remove_directory_job = RemoveDirectoryJob(directory_path, False, dependencies=[remove_file_job.id])

    s.schedule(create_directory_job)
    s.schedule(create_file_job)
    s.schedule(append_file_job)
    s.schedule(read_file_job)
    s.schedule(remove_file_job)
    s.schedule(remove_directory_job)

    s.run()


def network_scenario():
    logging.debug("======> Network")

    s = Scheduler()

    url = "https://worldtimeapi.org/api/timezone/Europe/Moscow"
    directory_path = os.path.join("output", "network")
    file_name = "response.txt"
    file_path = os.path.join(directory_path, file_name)

    create_directory_job = CreateDirectoryJob(directory_path)
    url_request_job = UrlRequestJob(url)

    def write_response():
        with open(file_path, "w") as f:
            f.write(url_request_job.result)

    write_response_job = SimpleSyncJob(write_response, dependencies=[url_request_job.id, create_directory_job.id])

    s.schedule(create_directory_job)
    s.schedule(url_request_job)
    s.schedule(write_response_job)

    s.run()


if __name__ == "__main__":
    init_logging()

    naked_jobs_scenario()
    scheduler_scenario()
    files_scenario()
    network_scenario()

    logging.debug("Exit")

import os
import shlex
import shutil
import subprocess
import time
from abc import abstractmethod
from warnings import warn

import b2luigi
from b2luigi.basf2_helper.utils import get_basf2_git_hash
from b2luigi.batch.processes import JobStatus
from b2luigi.core.task import Task
from tabulate import tabulate

import basf2
import basf2.pickle_path as b2pp


class Gbasf2PathTask(Task):
    # luigi parameters
    release = b2luigi.Parameter(default=get_basf2_git_hash())
    project_name = b2luigi.Parameter()
    input_dataset = b2luigi.Parameter(hashed=True)
    query_time_intervall = b2luigi.IntParameter(
        significant=False, default=120,
        description="Number of seconds to wait between each query whether job is done."
    )
    max_event = b2luigi.IntParameter(significant=False, default=0)

    # static properties that can be used to customize class when implementing subclass
    pickle_file_name = "serialized_path.pickle"
    wrapper_file_name = "process_pickled_path.py"
    #: subdirectory into which to download gbasf2 results
    dataset_download_directory = "gbasf2_results"

    gbasf2_setup_script = "setup_gbasf2.sh"

    @abstractmethod
    def create_path(self):
        "Function that returns a basf2 path to process on the grid"

    #: cached gbasf2 enviromnent, initiallized and accessed via ``self.env``
    _cached_env = None

    @property
    def env(self):
        """
        Return dictionary with gbasf2 environment from ``self.gbasf2_setup_script``.

        Runs the script only once and then caches the result in ``self._cached_env``
        to prevent unnecessary password prompts.
        """
        if self._cached_env is None:
            if not os.path.isfile(self.gbasf2_setup_script):
                raise FileNotFoundError("Use must provide a gbasf2 setup script \"{self.gbasf2_setup_script}\"")
            print(f"Setting up environment by sourcing {self.gbasf2_setup_script}")
            command = shlex.split(f"env -i bash -c 'source {self.gbasf2_setup_script} > /dev/null && env'")
            output = subprocess.check_output(command, encoding="utf-8")
            self._cached_env = dict(line.split("=", 1) for line in output.splitlines())
        return self._cached_env

    def run(self):
        self._write_path_to_file()
        self._create_wrapper_steering_file()
        if self._check_project_exists():
            print(f"Finished project \"{self.project_name}\" already exists.")
        else:
            self._send_to_grid()

        while self.get_project_status() in {JobStatus.running, JobStatus.idle}:
            time.sleep(self.query_time_intervall)
        if self.get_project_status() == JobStatus.successful:
            self._download_dataset(self.dataset_download_directory)
        else:
            warn("Project unsuccessful")

    def on_failure(self, exception):
        """Remove output fields so that task is not mistaken as done in case of failure"""
        wrapper_file_path = self.get_output_file_name(self.wrapper_file_name)
        if os.path.exists(wrapper_file_path):
            os.remove(wrapper_file_path)
        pickle_file_path = self.get_output_file_name(self.pickle_file_name)
        if os.path.exists(pickle_file_path):
            os.remove(pickle_file_path)
        shutil.rmtree(self.get_output_file_name(self.dataset_download_directory), ignore_errors=True)
        super().on_failure(exception)

    def output(self):
        yield self.add_to_output(self.pickle_file_name)
        yield self.add_to_output(self.wrapper_file_name)
        yield self.add_to_output(self.dataset_download_directory)

    def _write_path_to_file(self):
        path = self.create_path()
        path.add_module("Progress")
        pickle_file_path = self.get_output_file_name(self.pickle_file_name)
        b2pp.write_path_to_file(path, pickle_file_path)
        print(f"\nSaved serialized path in {pickle_file_path}\nwith content:\n")
        basf2.print_path(path)

    def _create_wrapper_steering_file(self):
        pickle_file_path = self.get_output_file_name(self.pickle_file_name)
        wrapper_file_path = self.get_output_file_name(self.wrapper_file_name)
        with open(wrapper_file_path, "w") as wrapper_file:
            wrapper_file.write(f"""
import basf2
from basf2 import pickle_path as b2pp
path = b2pp.get_path_from_file("{pickle_file_path}")
basf2.print_path(path)
basf2.process(path, max_event={self.max_event})
print(basf2.statistics)
            """
                               )

    def _send_to_grid(self):
        pickle_file_path = self.get_output_file_name(self.pickle_file_name)
        wrapper_file_path = self.get_output_file_name(self.wrapper_file_name)
        command_str = (f"gbasf2 {wrapper_file_path} -f {pickle_file_path} -i {self.input_dataset} "
                       f" -p {self.project_name} -s {self.release}")
        command = shlex.split(command_str)
        print(f"\nSending jobs to grid via command:\n{command_str}\n")
        subprocess.run(command, check=True, env=self.env, encoding="utf-8")

    def _check_project_exists(self):
        command = shlex.split(f"gb2_job_status -p {self.project_name}")
        output = subprocess.check_output(command, encoding="utf-8", env=self.env).strip()
        if output == "0 jobs are selected.":
            return False
        if "--- Summary of Selected Jobs ---" in output:
            return True
        raise RuntimeError("Output of gb2_job_status did not contain expected strings,"
                           " could not determine if project exists")

    def _get_n_jobs_by_status(self):
        assert self._check_project_exists(), f"Project {self.project_name} doest not exist yet"

        command = shlex.split(f"gb2_job_status -p {self.project_name}")
        output = subprocess.check_output(command, encoding="utf-8", env=self.env)
        # get job summary dict in the form of e.g.
        # {'Completed': 0, 'Deleted': 0, 'Done': 255, 'Failed': 0,
        # 'Killed': 0, 'Running': 0, 'Stalled': 0, 'Waiting': 0}
        job_summary_string = output.splitlines()[-1].strip()
        print("Job summary:\n" + job_summary_string)
        n_jobs_by_status = dict((summary_substring.split(":", 1)[0].strip(),
                                 int(summary_substring.split(":", 1)[1].strip()))
                                for summary_substring in job_summary_string.split())
        status_keys = list(n_jobs_by_status.keys())
        status_keys.sort()
        assert status_keys == ['Completed', 'Deleted', 'Done', 'Failed',
                               'Killed', 'Running', 'Stalled', 'Waiting'],\
            "Error when obtaining job summary, it does not contain the required status keys"
        return n_jobs_by_status

    def get_project_status(self):
        """
        Return ``True`` if all jobs are done, ``False`` if some jobs are still being
        processed and raise error if some jobs failed.
        """
        print(f"Checking if jobs for project {self.project_name} are complete.")

        n_jobs_by_status = self._get_n_jobs_by_status()
        n_jobs = sum(n_jobs_by_status.values())
        n_done = n_jobs_by_status["Done"]
        n_failed = (n_jobs_by_status["Failed"] +
                    n_jobs_by_status["Killed"] +
                    n_jobs_by_status["Deleted"] +
                    n_jobs_by_status["Stalled"])
        n_being_processed = n_jobs_by_status["Running"] + n_jobs_by_status["Completed"]
        n_waiting = n_jobs_by_status["Waiting"]
        assert n_failed + n_done + n_waiting + n_being_processed == n_jobs,\
            "Error in job categorization, numbers of jobs in cateries don't add up to total"
        # TODO think what to do with partially failed projects, maybe resubmit?

        # if n_done > 0 and n_being_processed == 0:
        # return JobStatus.successful
        # if n_failed == n_jobs:
        #     return JobStatus.aborted
        if n_done == n_jobs:
            return JobStatus.successful
        if n_failed > 0:
            return JobStatus.aborted
        if n_waiting == n_jobs:
            return JobStatus.idle
        if n_being_processed > 0:
            return JobStatus.running
        raise RuntimeError("Could not determine JobStatus")

    def _download_dataset(self, output_directory):
        os.makedirs(output_directory, exist_ok=True)
        try:
            command = shlex.split(f"gb2_ds_get --force {self.project_name}")
            print("Downloading dataset with command ", " ".join(command))
            output = subprocess.check_output(command, env=self.env, encoding="utf-8", cwd=output_directory)
            print(output)
            if output.strip() == "No file found":
                raise RuntimeError(f"No output data for gbasf2 project {self.project_name} found.")
        except (subprocess.CalledProcessError, RuntimeError) as err:
            shutil.rmtree(output_directory)
            raise err

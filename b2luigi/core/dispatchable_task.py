import contextlib
import os
import subprocess
import sys
import types
import functools

import colorama

from b2luigi.core.settings import get_setting
from b2luigi.core.task import Task
from b2luigi.core.utils import get_log_files


def _on_failure_for_dispatch_task(self, exception):
    stdout_file_name, stderr_file_name = get_log_files(self)

    print(colorama.Fore.RED)
    print("Task", self.task_family, "failed!")
    print("Parameters")
    for key, value in self.get_filled_params().items():
        print("\t",key, "=", value)
    print("Please have a look into the log files")
    print(stdout_file_name)
    print(stderr_file_name)
    print(colorama.Style.RESET_ALL)

def _run_task_locally(task, run_function):
    task.create_output_dirs()
    run_function(task)

def _run_task_remote(task):
    task.on_failure = types.MethodType(_on_failure_for_dispatch_task, task)

    filename = os.path.realpath(sys.argv[0])
    stdout_file_name, stderr_file_name = get_log_files(task)

    cmd = []
    if hasattr(task, "cmd_prefix"):
        cmd = task.cmd_prefix

    executable = get_setting("executable", [sys.executable])
    cmd += executable
    
    cmd += [os.path.basename(filename), "--batch-runner", "--task-id", task.task_id]

    with open(stdout_file_name, "w") as stdout_file:
        with open(stderr_file_name, "w") as stderr_file:
            return_code = subprocess.call(cmd, stdout=stdout_file, stderr=stderr_file,
                                          cwd=os.path.dirname(filename))

    if return_code:
        raise RuntimeError(f"Execution failed with return code {return_code}")


def dispatch(run_function):
    """
    In cases you have a run function calling external, probably insecure functionalities,
    use this function wrapper around your run function.

    Example:
        The run function can include any code you want. When the task runs,
        it is started in a subprocess and monitored by the parent process.
        When it dies unexpectedly (e.g. because of a segfault etc.)
        the task will be marked as failed. If not, it is successful.
        The log output will be written to two files in the log folder (marked with 
        the parameters of the task), which you can check afterwards::

            import b2luigi


            class MyTask(b2luigi.Task):
                @b2luigi.dispatch
                def run(self):
                    call_some_evil_function()

    Implementation note:
        In the subprocess we are calling the current sys.executable (which should by python 
        hopefully) with the current input file as a parameter, but let it only run this
        specific task (by handing over the task id and the `--batch-worker` option).
        The run function notices this and actually runs the task instead of dispatching again.

    You have the possibility to control what exactly is used as executable
    by setting the "executable" setting, which needs to be a list of strings.
    Additionally, you can add a ``cmd_prefix`` parameter to your class, which also
    needs to be a list of strings, which are prefixed to the current command (e.g.
    if you want to add a profiler to all your tasks)
    """
    @functools.wraps(run_function)
    def wrapped_run_function(self):
        if get_setting("local_execution", False):
            _run_task_locally(self, run_function)
        else:
            _run_task_remote(self)

    return wrapped_run_function


class DispatchableTask(Task):
    """
    Instead of using the :obj:`dispatch` function wrapper,
    you can also inherit from this class.
    Except that, it has exactly the same functionality
    as a normal :obj:`Task`.

    Important: 
        You need to overload the process function
        instead of the run function in this case!
    """
    def process(self):
        """
        Override this method with your normal run function.
        Do not touch the run function itself!
        """
        raise NotImplementedError

    def run(self):
        dispatch(self.__class__.process)(self)

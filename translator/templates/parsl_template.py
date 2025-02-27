import parsl
import time
import multiprocessing
from pathlib import Path
import logging
from typing import List
from parsl.app.app import python_app, bash_app
from parsl.monitoring.monitoring import MonitoringHub
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.addresses import address_by_hostname
from parsl.data_provider.files import File


docker_htex = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_docker",
            # worker_debug=True,
            cores_per_worker=1,
            max_workers_per_node=1,
            provider=AdHocProvider(
                channels=[
                    SSHChannel(
                        hostname="localhost",
                        username="root",
                        port=2222
                    ),
                    SSHChannel(
                        hostname="localhost",
                        username="root",
                        port=2223
                    )
                ],
            ),
        )
    ],
    strategy=None,
    monitoring=MonitoringHub(
       hub_address=address_by_hostname(),
       monitoring_debug=False,
       resource_monitoring_interval=10,
   ),
)

local_htex = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_local",
            worker_debug=True,
            cores_per_worker=1,
            max_workers_per_node=4,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
            ),
        )
    ],
    strategy=None,
    monitoring=MonitoringHub(
        hub_address=address_by_hostname(),
        monitoring_debug=False,
        resource_monitoring_interval=10,
    ),
)

parsl.clear()
parsl.load(local_htex)
# Uncomment to use docker containers as workers
# parsl.load(docker_htex)

# Emit log lines to the screen
parsl.set_stream_logger(level=logging.DEBUG)

# Write log to file, specify level of detail for logs
# FILENAME = "debug.log"
# parsl.set_file_logger(FILENAME, level=logging.DEBUG)

@bash_app
def generic_shell_app(cmd: str, inputs=[], outputs=[], stdout="stdout.txt", stderr="stderr.txt", parsl_resource_specification=None):
    from pathlib import Path
    from parsl.data_provider.files import File
    #TODO: make sure file directory exists, if not, create them
    for i in inputs:
        if isinstance(i, File):
            input_path = Path(i.filepath)
            cmd = cmd.replace(input_path.name, i.filepath)
    for o in outputs:
        output_path = Path(o.filepath)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = cmd.replace(output_path.name, o.filepath)
    return cmd

@python_app
def barrier():
    return 0

current_workdir = Path.cwd()

file_map = {}

def get_parsl_files (filenames: List[str], is_output: bool = False) -> List[File]:
    parsl_files = []

    for filename in filenames:
        if filename not in file_map:
            file_folder = "data"
            if is_output:
                file_folder = "output"
            file_map[filename] = File(str(current_workdir.joinpath(f"{file_folder}/{filename}")))
        parsl_files.append(file_map[filename])
    
    return parsl_files

# Generated code goes here



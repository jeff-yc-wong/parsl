import parsl
import time
import argparse
from pathlib import Path
import logging
from typing import List
from parsl.app.app import python_app, bash_app
from parsl.monitoring.monitoring import MonitoringHub
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider, AdHocProvider
from parsl.channels import LocalChannel, SSHChannel
from parsl.addresses import address_by_hostname
from parsl.data_provider.files import File

parser = argparse.ArgumentParser(description="Run a parsl workflow.")

parser.add_argument("--docker", action="store_true", help="Run the workflow in a docker container.")
parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity.")
parser.add_argument("--num_workers", default=2, type=int, help="The number of workers to use.")

args = parser.parse_args()

label="htex_local"
provider=LocalProvider(
    channel=LocalChannel(),
    init_blocks=args.num_workers,
    max_blocks=args.num_workers,
)

if args.docker:
    channels = []

    for i in range(args.num_workers):
        channel = SSHChannel(
            hostname="localhost",
            username="parsl",
            port=2222+i,
        )
        channels.append(channel)

    label="htex_docker"
    provider=AdHocProvider(
        channels=channels,
    )


config = Config(
    executors=[
        HighThroughputExecutor(
            label=label,
            worker_debug=True,
            cores_per_worker=1,
            max_workers_per_node=1,
            worker_logdir_root="logs",
            provider=provider,
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
parsl.load(config)

if args.verbose:
    # Emit log lines to the screen
    parsl.set_stream_logger(level=logging.DEBUG)

# Write log to file, specify level of detail for logs
FILENAME = "parsl_debug.log"
parsl.set_file_logger(FILENAME, level=logging.DEBUG)


@bash_app
def generic_shell_app(cmd: str, file_inputs=[],  inputs=[], outputs=[], stdout="stdout.txt", stderr="stderr.txt"):
    from pathlib import Path
    from parsl.data_provider.files import File

    # replace filepaths using regex (?:^|(?<=\s)|(?<=['"])|(?<=\\")|(?<=\\'))path_name(?:$|(?=\s)|(?=['"])|(?=\\"|\\'))

    file_inputs = sorted(file_inputs, key=lambda x: -len(x.filepath))
    outputs = sorted(outputs, key=lambda x: -len(x.filepath))

    for i in file_inputs:
        if isinstance(i, File):
            input_path = Path(i.filepath)
            cmd = cmd.replace(input_path.name, i.filepath)
    for o in outputs:
        if isinstance(o, File):
            output_path = Path(o.filepath)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            cmd = cmd.replace(output_path.name, o.filepath)
    return cmd


@python_app
def barrier():
    return 0


current_workdir = Path.cwd()

file_map = {}

def get_parsl_files(filenames: List[str], is_output: bool = False) -> List[File]:
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

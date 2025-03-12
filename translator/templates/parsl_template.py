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
from parsl.executors.high_throughput.manager_selector import MostIdleSelector, FastestManagerSelector, RandomManagerSelector


parser = argparse.ArgumentParser(description="Run a parsl workflow.")

parser.add_argument("--docker", action="store_true", help="Run the workflow in a docker container.")
parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity.")
parser.add_argument("-tss", "--task_selection_scheme", default="fcfs", type=str, help="The task selection scheme to use.")
parser.add_argument("-wss", "--worker_selection_scheme", default="random", type=str, help="The worker selection scheme to use.")
parser.add_argument("-m", "--metric", default="makespan", type=str, help="The metric to optimize for.")
parser.add_argument("-c", "--calibration", default=None, type=str, help="The calibration file to use.")
parser.add_argument("-n", "--num_threads", default=1, type=int, help="The number of threads to use.")
parser.add_argument("--simulate", action="store_true", help="Run the workflow in simulation mode.")
parser.add_argument("--num_workers", default=2, type=int, help="The number of workers to use.")

args = parser.parse_args()

possible_task_params = {"fcfs": None, "most_data": "data_size", "most_flops": "computation",
                        "most_children": "num_children", "highest_bottom_level": "bottom_level"}
possible_managers = {"most_idle_cores": MostIdleSelector(), 
                     "fastest_cores": FastestManagerSelector(), "random": RandomManagerSelector()}

print(
f"""Running workflow with the following algorihtms:
    Task selection scheme: {args.task_selection_scheme}
    Worker selection scheme: {args.worker_selection_scheme}
""")

scheduling_config = {}

if args.simulate:
    scheduling_config = {
        "workflow_file": str(Path("./jsons/workflow.json").absolute()),
        "simulator_path": "workflow_simulator",
        "template": str(Path("./jsons/template.json").absolute()),
        "metric": args.metric,
        "num_threads": args.num_threads,
        "verbose": args.verbose,
        "calibration": args.calibration,
    }

channels = []

for i in range(args.num_workers):
    channel = SSHChannel(
        hostname="localhost",
        username="root",
        port=2222+i,
    )
    channels.append(channel)

docker_htex = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_docker",
            worker_debug=True,
            cores_per_worker=1,
            max_workers_per_node=1,
            provider=AdHocProvider(
                channels=channels,
            ),
            manager_selector=possible_managers[args.worker_selection_scheme],
            task_selector=possible_task_params[args.task_selection_scheme],
            **scheduling_config
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
            max_workers_per_node=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=args.num_workers,
                max_blocks=args.num_workers,
            ),
            manager_selector=possible_managers[args.worker_selection_scheme],
            task_selector=possible_task_params[args.task_selection_scheme],
            **scheduling_config
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

if args.docker:
    parsl.load(docker_htex)
else:
    parsl.load(local_htex)

if args.verbose:
    # Emit log lines to the screen
    parsl.set_stream_logger(level=logging.DEBUG)

# Write log to file, specify level of detail for logs
FILENAME = "parsl_debug.log"
parsl.set_file_logger(FILENAME, level=logging.DEBUG)


@bash_app
def generic_shell_app(cmd: str, file_inputs=[],  inputs=[], outputs=[], stdout="stdout.txt", stderr="stderr.txt", parsl_resource_specification=None):
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

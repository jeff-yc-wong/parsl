import parsl
import time
import argparse
import json
from pathlib import Path
import logging
import ipaddress
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

def parse_ip(ip_str):
    try:
        return ipaddress.ip_address(ip_str)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid IP address: {ip_str}")

def validate_worker_config(args):
    """Validate and normalize worker configuration based on IPs and num_workers."""
    if args.ip is None:
        # Local execution - use first num_workers value or default
        if isinstance(args.num_workers, list):
            return args.num_workers[0] if args.num_workers else 2
        return args.num_workers
    
    # Remote execution with IPs
    num_ips = len(args.ip)
    
    if isinstance(args.num_workers, list):
        if len(args.num_workers) == 1:
            # Single value for all IPs
            return [args.num_workers[0]] * num_ips
        elif len(args.num_workers) == num_ips:
            # One value per IP
            return args.num_workers
        else:
            raise argparse.ArgumentTypeError(
                f"Number of worker counts ({len(args.num_workers)}) must be 1 or match number of IPs ({num_ips})"
            )
    else:
        # Single integer for all IPs
        return [args.num_workers] * num_ips

parser = argparse.ArgumentParser(description="Run a parsl workflow.")

parser.add_argument("--docker", action="store_true", help="Run the workflow in a docker container.")
parser.add_argument("-wd", "--worker-debug", action="store_true", help="Turn on worker debug logs")
parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity.")
parser.add_argument("-tss", "--task_selection_scheme", default="fcfs", type=str, help="The task selection scheme to use.")
parser.add_argument("-wss", "--worker_selection_scheme", default="random", type=str, help="The worker selection scheme to use.")
parser.add_argument("-m", "--metric", default="makespan", type=str, help="The metric to optimize for.")
parser.add_argument("-c", "--calibration", default=None, type=str, help="The calibration file to use.")
parser.add_argument("-n", "--num_threads", default=1, type=int, help="The number of threads to use.")
parser.add_argument("--simulate", action="store_true", help="Run the workflow in simulation mode.")
parser.add_argument("--ip", type=parse_ip, nargs='*', help="A space separated list of IP addresses to use for workers")
parser.add_argument("--num_workers", nargs='*', default=2, type=int, 
                    help="Number of workers to start. Can be a single integer (applied to all IPs) or a list of integers (one per IP).")
parser.add_argument("--workflow_file", default=str(Path("./jsons/workflow.json").absolute()), type=str, help="The path to the WfFormat workflow json")

args = parser.parse_args()

# Validate and get worker configuration
worker_config = validate_worker_config(args)

possible_managers = {"most_idle_cores": MostIdleSelector(),
                     "fastest_cores": FastestManagerSelector(), "random": RandomManagerSelector()}

print(
f"""Running workflow with the following algorithms:
    Task selection scheme: {args.task_selection_scheme}
    Worker selection scheme: {args.worker_selection_scheme} ({possible_managers[args.worker_selection_scheme]})
""")

scheduling_config = {}

current_workdir = Path.cwd()
relative_to_home = current_workdir.relative_to(Path.home())

if args.simulate and args.workflow_file is not None:

    if args.calibration is not None:
        calibration = json.loads(args.calibration)

    scheduling_config = {
        "workflow_file": args.workflow_file,
        "simulator_path": "workflow_simulator",
        "template": str(Path("./jsons/template.json").absolute()),
        "metric": args.metric,
        "num_threads": args.num_threads,
        "verbose": args.verbose,
        "calibration": calibration,
    }

label="htex_local"
provider=LocalProvider(
    channel=LocalChannel(),
    init_blocks=worker_config if isinstance(worker_config, int) else worker_config[0],
    max_blocks=worker_config if isinstance(worker_config, int) else worker_config[0],
)

if args.docker:
    channels = []
    
    if args.ip is not None:
        for index, ip in enumerate(args.ip):
            num_workers_for_ip = worker_config[index]
            for i in range(num_workers_for_ip):
                channel = SSHChannel(
                    hostname=str(ip),
                    username="parsl",
                    port=2222+i,
                )
                channels.append(channel)
    else:
        num_workers_local = worker_config if isinstance(worker_config, int) else worker_config[0]
        for i in range(num_workers_local):
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

total_workers = sum(worker_config) if isinstance(worker_config, list) else worker_config

config = Config(
    executors=[
        HighThroughputExecutor(
            label=label,
            worker_debug=args.worker_debug,
            cores_per_worker=1,
            max_workers_per_node=1,
            num_workers=total_workers,
            num_tasks = # replace num_tasks here,
            worker_logdir_root=str(relative_to_home.joinpath("logs")) if args.worker_debug  else "logs",
            provider=provider,
            manager_selector=possible_managers[args.worker_selection_scheme],
            task_selector=args.task_selection_scheme,
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
parsl.load(config)

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

file_map = {}

def get_parsl_files(filenames: List[str], is_output: bool = False) -> List[File]:
    parsl_files = []

    for filename in filenames:
        if filename not in file_map:
            file_folder = "data"
            if is_output:
                file_folder = "output"
            file_map[filename] = File(str(relative_to_home.joinpath(f"{file_folder}/{filename}")))
        parsl_files.append(file_map[filename])

    return parsl_files

# Generated code goes here

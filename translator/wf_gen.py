import pathlib
import os
import argparse
import copy
import subprocess
import math
import platform
import logging
import psutil

from wfcommons import BlastRecipe, MontageRecipe, SoykbRecipe, EpigenomicsRecipe
from wfcommons import GenomeRecipe, BwaRecipe, CyclesRecipe, SrasearchRecipe
from wfcommons.wfbench import WorkflowBenchmark
from wfcommons.wfinstances import Instance
from wfcommons.common import Machine, MachineSystem

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change this to control the verbosity
    format="[wf_gen][%(asctime)s][%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler()]
)

def log_info(msg: str):
    """
    Log an info message to stderr

    :param msg:
    :type msg: str
    """
    logging.info(msg)

def log_debug(msg: str):
    """
    Log a debug message to stderr

    :param msg:
    :type msg: str
    """
    logging.debug(msg)

def log_error(msg: str):
    """
    Log a error message to stderr

    :param msg:
    :type msg: str
    """
    logging.error(msg)


recipes = {
    "makeflow-blast-medium": BlastRecipe,
    "1000genome-20200402T023420Z-0": GenomeRecipe,
    "makeflow-bwa-large": BwaRecipe,
    "Cycles-20200413T220633Z-0": CyclesRecipe,
    "genome-dax-0": EpigenomicsRecipe,
    "montage": MontageRecipe,
    "soykb-0": SoykbRecipe,
    "workflow-test": SrasearchRecipe
}

def write_benchmark(workflow_path: pathlib.Path, cpu_bench_ref: float = 1.0, scale: float = 1.0, system_info: Machine = None, cpu_only: bool = False, io_only: bool = False):
    # create a workflow benchmark from a synthetic workflow (workflow used in the fgcs paper)
    workflow_instance = Instance(workflow_path)
    workflow = workflow_instance.workflow
    cpu_work = {}
    runtimes = {}

    if cpu_only and io_only:
        raise ValueError("Cannot create a benchmark with both cpu_only and io_only set to True")

    workflow_name = str(workflow_path).rsplit("/", 1)[1].split("-", 1)[0]

    workflow.makespan = 0

    for task in workflow.tasks.values():
        task.category = task.task_id
        if not task.avg_cpu and not task.cores:
            task.avg_cpu = 100
            task.cores = 1
        elif not task.cores:
            task.cores = math.ceil(task.avg_cpu / 100)
            log_debug(f"[WARNING]: Task {task.task_id} has avgCPU but not cores. Assuming {task.cores} cores.")
        elif not task.avg_cpu:
            task.avg_cpu = 100
            log_debug(f"[WARNING]: Task {task.task_id} has cores but not avgCPU. Assuming 100% avgCPU.")
        elif task.avg_cpu > 100 * task.cores:
            task.avg_cpu = 100 * task.cores
            log_debug(f"[WARNING]: Task {task.task_id} specifies {task.cores} cores and {task.avg_cpu}% avgCPU which is impossible: Assuming avgCPU to {task.avg_cpu}% instead.")


        # cpu time

        if not io_only:
            runtimes[task.task_id] = task.runtime * task.avg_cpu / (100 * task.cores) * scale

            cpu_work[task.category] =  runtimes[task.task_id] / cpu_bench_ref * 100
        else:
            runtimes[task.task_id] = cpu_bench_ref / 100
            cpu_work[task.category] = 1

        task.memory = None
        task.avg_cpu = 100
        task.cores = 1

    if workflow.name not in recipes:
        log_info(f"Recipe for {workflow.name} not found.")
        return
    benchmark = WorkflowBenchmark(recipe=recipes[workflow.name], num_tasks=len(workflow.tasks))
    benchmark.workflow = copy.deepcopy(workflow)
    if cpu_only:
        output_path = pathlib.Path(f"./benchmarks/{workflow_name}_{scale}_cpu")
    elif io_only:
        output_path = pathlib.Path(f"./benchmarks/{workflow_name}_{scale}_io")
    else:
        output_path = pathlib.Path(f"./benchmarks/{workflow_name}_{scale}")

    path = benchmark.create_benchmark(output_path, percent_cpu=1.0, cpu_work=cpu_work, regenerate=False)

    for key, task in benchmark.workflow.tasks.items():

        
        task.output_files = workflow.tasks[key].output_files
        task.input_files = workflow.tasks[key].input_files

        for file in task.output_files:
            file.size = 0 if cpu_only else math.ceil(file.size * scale)

        for file in task.input_files:
            file.size = 0 if cpu_only else math.ceil(file.size * scale)

    
        output_files = {file.file_id: file.size for file in task.output_files}
        input_files = [file.file_id for file in task.input_files]

        task.args.append(f"--output-files {output_files}")
        task.args.append(f"--input-files {input_files}")


        assert round(runtimes[task.task_id], 4), round((cpu_work[task.category] / 100) * cpu_bench_ref, 4)

        task.runtime = round(runtimes[task.task_id], 4)

        if system_info:
            task.machines = [system_info]

    benchmark.workflow.name = f"{workflow_name}_{scale}"

    if cpu_only:
        benchmark.workflow.name += "_cpu"
    elif io_only:
        benchmark.workflow.name += "_io"

    benchmark._rename_files_to_wfbench_format()

    os.remove(path)
    benchmark.workflow.write_json(output_path.joinpath(benchmark.workflow.name + ".json"))

def main():
    parser = argparse.ArgumentParser(description="Process a workflow JSON file.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--workflow", type=str, help="Path to the workflow JSON file.")
    group.add_argument("--all", action="store_true", help="Create benchmarks for all workflows")
    parser.add_argument("--test", action="store_true", help="Create a test benchmark")
    parser.add_argument("--scale", type=float, default=1.0, help="Scale the CPU work")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--cpu-only", action="store_true", help="Create a benchmark with only CPU tasks")
    parser.add_argument("--io-only", action="store_true", help="Create a benchmark with only IO tasks")
    parser.add_argument("--ref", type=float, default=None, help="Define how many seconds per 100 cpu-work")
    
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if not platform.system().lower().startswith("linux"):
        log_info("[WARNING]: taskset is not available, benchmarking cpu-benchmark instead of wfbench (this might be less accurate)")

    if platform.system().lower().startswith("linux"):
        cmd = ["taskset", "-c", "0", "bash", "-c","TIMEFORMAT='%3R'; time wfbench --cpu-work 100 --percent-cpu 1.0 --name bench &> /dev/null"]
    else:
        cmd = ["bash", "-c","TIMEFORMAT='%3R'; time cpu-benchmark 100"]

    if not args.ref:
        ref_sum = 0

        for i in range(10):
            print(f"Benchmarking Wfbench/cpu-benchmark {i+1}/10", end="\r")
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

            ref_sum += float(result.stderr.decode("utf-8"))

        ref = ref_sum / 10
    else:
        ref = args.ref

    log_info(f"Reference CPU benchmark: {ref}s per 100 cpu-work")

    cpu_info = {
            "coreCount": os.cpu_count(),
            "vendor": platform.processor()
        }

    if psutil.cpu_freq().max:
        cpu_info["speedInMHz"] =  int(psutil.cpu_freq().max)
    # Get system information
    system_info = Machine(
            name=platform.node(),
            cpu = cpu_info,
            system= MachineSystem('macos') if platform.system() == "Darwin" else MachineSystem(platform.system().lower()),
            architecture=platform.machine(),
            release=platform.release(),
            memory=psutil.virtual_memory().total
        )
    

    if args.test or args.all:
        workflow = Instance("./workflows/montage-chameleon-2mass-005d-001.json").workflow
        output_path = pathlib.Path("./benchmarks/test")
        benchmark = WorkflowBenchmark(recipe=MontageRecipe, num_tasks=len(workflow.tasks))
        benchmark.workflow = copy.deepcopy(workflow)
        benchmark.workflow.name = "test"
        path = benchmark.create_benchmark(output_path, percent_cpu=1.0, cpu_work=100, regenerate=False, data=10)

        benchmark.workflow.makespan = 0

        for task in benchmark.workflow.tasks.values():
            task.runtime = round(ref, 4)
            task.machines = [system_info]

        benchmark.workflow.write_json(path)
    if args.workflow:
        write_benchmark(args.workflow, ref, args.scale, system_info, False, False)
        if args.cpu_only:
            write_benchmark(args.workflow, ref, args.scale, system_info, True, False)
        if args.io_only:
            write_benchmark(args.workflow, ref, args.scale, system_info, False, True)
        
    if args.all:
        all_path = pathlib.Path("./workflows")

        for file in all_path.iterdir():
            if file.suffix == ".json":
                write_benchmark(file, ref, args.scale, system_info, False, False)

                if args.cpu_only:
                    write_benchmark(file, ref, args.scale, system_info, True, False)
                if args.io_only:
                    write_benchmark(file, ref, args.scale, system_info, False, True)

if __name__ == "__main__":
    main()


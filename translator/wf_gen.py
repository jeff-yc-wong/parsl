import pathlib
import argparse
import copy
import subprocess
import math
import platform
import logging

from wfcommons import BlastRecipe, MontageRecipe, SoykbRecipe, EpigenomicsRecipe
from wfcommons import GenomeRecipe, BwaRecipe, CyclesRecipe, SrasearchRecipe
from wfcommons.wfbench import WorkflowBenchmark
from wfcommons.wfinstances import Instance
from wfcommons.common import Task

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

def write_benchmark(workflow_path: str, cpu_bench_ref: float = 1.0, scale: float = 1.0):
    # create a workflow benchmark from a synthetic workflow (workflow used in the fgcs paper)
    workflow_instance = Instance(workflow_path)
    workflow = workflow_instance.workflow
    cpu_work = {}
    runtimes = {}

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
        runtimes[task.task_id] = task.runtime * task.avg_cpu / (100 * task.cores) * scale

        cpu_work[task.category] =  runtimes[task.task_id] / cpu_bench_ref * 100
        task.memory = None
        task.avg_cpu = 100
        task.cores = 1

    if workflow.name not in recipes:
        log_info(f"Recipe for {workflow.name} not found.")
        return
    benchmark = WorkflowBenchmark(recipe=recipes[workflow.name], num_tasks=len(workflow.tasks))
    benchmark.workflow = copy.deepcopy(workflow)
    output_path = pathlib.Path(f"./benchmarks/{workflow.name}")
    path = benchmark.create_benchmark(output_path, percent_cpu=1.0, cpu_work=cpu_work, regenerate=False)

    for key, task in benchmark.workflow.tasks.items():
        task.output_files = workflow.tasks[key].output_files
        task.input_files = workflow.tasks[key].input_files

        output_files = {file.file_id: file.size for file in task.output_files}
        input_files = {file.file_id: file.size for file in task.input_files}

        task.args.append(f"--output-files {output_files}")
        task.args.append(f"--input-files {input_files}")


        assert round(runtimes[task.task_id], 4), round((cpu_work[task.category] / 100) * cpu_bench_ref, 4)

        task.runtime = runtimes[task.task_id]

    benchmark._rename_files_to_wfbench_format()

    benchmark.workflow.write_json(path)

def main():
    parser = argparse.ArgumentParser(description="Process a workflow JSON file.")
    parser.add_argument("--workflow", type=str, help="Path to the workflow JSON file.")
    parser.add_argument("--test", action="store_true", help="Create a test benchmark")
    parser.add_argument("--all", action="store_true", help="Create benchmarks for all workflows")
    parser.add_argument("--scale", type=float, default=1.0, help="Scale the CPU work")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if not platform.system().lower().startswith("linux"):
        log_info("[WARNING]: taskset is not available, benchmarking cpu-benchmark instead of wfbench (this might be less accurate)")

    if platform.system().lower().startswith("linux"):
        cmd = ["bash", "-c","TIMEFORMAT='%3R'; time wfbench --cpu-work 100 --percent-cpu 1.0 --name bench &> /dev/null"]
    else:
        cmd = ["bash", "-c","TIMEFORMAT='%3R'; time cpu-benchmark 100"]

    ref_sum = 0

    for i in range(10):
        print(f"Benchmarking Wfbench/cpu-benchmark {i+1}/10", end="\r")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)

        ref_sum += float(result.stderr.decode("utf-8"))

    ref = ref_sum / 10

    log_info(f"Reference CPU benchmark: {ref}s per 100 cpu-work")

    if args.test:
        write_benchmark("./workflows/montage-chameleon-2mass-005d-001.json", ref, args.scale)

        workflow = Instance("./workflows/montage-chameleon-2mass-005d-001.json").workflow
        output_path = pathlib.Path("./benchmarks/test")
        benchmark = WorkflowBenchmark(recipe=MontageRecipe, num_tasks=len(workflow.tasks))
        benchmark.workflow.name = "test"
        path = benchmark.create_benchmark(output_path, percent_cpu=1.0, cpu_work=100, regenerate=False, data=10)

        ## create a workflow benchmark object to generate specifications based on a Montage recipe (small benchmark for testing)
        # workflow_instance = Instance("./workflows/montage-chameleon-2mass-005d-001.json")
        # benchmark = WorkflowBenchmark(recipe=MontageRecipe, num_tasks=len(workflow_instance.workflow.tasks))
        # output_path = pathlib.Path(f"./benchmarks/{workflow_instance.name}")
        # path = benchmark.create_benchmark_from_synthetic_workflow(output_path, workflow_instance.workflow)

    
    if args.workflow:
        write_benchmark(args.workflow, ref, args.scale)
        
    if args.all:
        all_path = pathlib.Path("./workflows")

        for file in all_path.iterdir():
            if file.suffix == ".json":
                write_benchmark(file, ref, args.scale)

if __name__ == "__main__":
    main()


import pathlib
import argparse
import copy

from wfcommons import BlastRecipe, MontageRecipe, SoykbRecipe, EpigenomicsRecipe
from wfcommons import GenomeRecipe, BwaRecipe, CyclesRecipe, SrasearchRecipe
from wfcommons.wfbench import WorkflowBenchmark
from wfcommons.wfinstances import Instance


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

def write_benchmark(workflow_path: str):
    # create a workflow benchmark from a synthetic workflow (workflow used in the fgcs paper)
    workflow_instance = Instance(workflow_path)
    workflow = workflow_instance.workflow
    cpu_work = {}
    for task in workflow.tasks.values():
        task.category = task.task_id
        cpu_work[task.category] = (task.runtime * task.avg_cpu / 100) * 1000
        task.avg_cpu = None
        task.memory = None

    print(cpu_work)

    if workflow.name not in recipes:
        print(f"Recipe for {workflow.name} not found.")
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

    benchmark._rename_files_to_wfbench_format()

    benchmark.workflow.write_json(path)


def main():
    parser = argparse.ArgumentParser(description="Process a workflow JSON file.")
    parser.add_argument("--workflow", type=str, help="Path to the workflow JSON file.")
    parser.add_argument("--test", action="store_true", help="Create a test benchmark")
    parser.add_argument("--all", action="store_true", help="Create benchmarks for all workflows")
    
    args = parser.parse_args()

    if args.test:
        workflow = Instance("./workflows/montage-chameleon-2mass-005d-001.json").workflow
        cpu_work = {}
        for task in workflow.tasks.values():
            task.category = task.task_id
            cpu_work[task.category] = task.runtime * task.avg_cpu * 100
            task.avg_cpu = None
            task.memory = None

        workflow.write_json("./test.json")
        benchmark = WorkflowBenchmark(recipe=MontageRecipe, num_tasks=len(workflow.tasks))
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

        benchmark._rename_files_to_wfbench_format()

        benchmark.workflow.write_json(path)

        output_path = pathlib.Path("./benchmarks/test")
        benchmark.workflow.name = "test"
        path = benchmark.create_benchmark(output_path, percent_cpu=1.0, cpu_work=100, regenerate=False, data=10)

        ## create a workflow benchmark object to generate specifications based on a Montage recipe (small benchmark for testing)
        # workflow_instance = Instance("./workflows/montage-chameleon-2mass-005d-001.json")
        # benchmark = WorkflowBenchmark(recipe=MontageRecipe, num_tasks=len(workflow_instance.workflow.tasks))
        # output_path = pathlib.Path(f"./benchmarks/{workflow_instance.name}")
        # path = benchmark.create_benchmark_from_synthetic_workflow(output_path, workflow_instance.workflow)
    if args.workflow:
        write_benchmark(args.workflow)
        
    if args.all:
        all_path = pathlib.Path("./workflows")

        for file in all_path.iterdir():
            if file.suffix == ".json":
                write_benchmark(file)

if __name__ == "__main__":
    main()


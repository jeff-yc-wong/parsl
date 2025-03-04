import pathlib

from wfcommons import BlastRecipe, MontageRecipe, SoykbRecipe
from wfcommons.wfbench import WorkflowBenchmark, DaskTranslator, CWLTranslator, ParslTranslator
from wfcommons.wfinstances import Instance
from wfcommons import WorkflowGenerator


import argparse
import json
import copy

def main():
    parser = argparse.ArgumentParser(description="Process a workflow JSON file.")
    parser.add_argument("--workflow", type=str, help="Path to the workflow JSON file.")
    parser.add_argument("--test", action="store_true", help="Create a test benchmark")
    
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
        path = benchmark.create_benchmark(output_path, percent_cpu=1.0, cpu_work=100, regenerate=False, data=10)

        ## create a workflow benchmark object to generate specifications based on a Montage recipe (small benchmark for testing)
        # workflow_instance = Instance("./workflows/montage-chameleon-2mass-005d-001.json")
        # benchmark = WorkflowBenchmark(recipe=MontageRecipe, num_tasks=len(workflow_instance.workflow.tasks))
        # output_path = pathlib.Path(f"./benchmarks/{workflow_instance.name}")
        # path = benchmark.create_benchmark_from_synthetic_workflow(output_path, workflow_instance.workflow)
    if args.workflow:
        # create a workflow benchmark from a synthetic workflow (workflow used in the fgcs paper)
        workflow_instance = Instance(args.workflow)
        benchmark = WorkflowBenchmark(recipe=SoykbRecipe, num_tasks=156)
        output_path = pathlib.Path(f"./benchmarks/{workflow_instance.name}")
        path = benchmark.create_benchmark_from_synthetic_workflow(output_path, workflow_instance.workflow)
    

if __name__ == "__main__":
    main()


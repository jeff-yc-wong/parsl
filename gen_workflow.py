import pathlib

from wfcommons import BlastRecipe, MontageRecipe
from wfcommons.wfbench import WorkflowBenchmark, DaskTranslator, ParslTranslator
from wfcommons.wfinstances import Instance

# create a workflow benchmark object to generate specifications based on a recipe
#benchmark = WorkflowBenchmark(recipe=BlastRecipe, num_tasks=45)
# generate a specification based on performance characteristics
#path = benchmark.create_benchmark(pathlib.Path("./blast_base"), cpu_work=100, data=10, percent_cpu=0.6)

# create a workflow benchmark object to generate specifications based on a Montage recipe
benchmark = WorkflowBenchmark(recipe=MontageRecipe, num_tasks=60)
# generate a specification based on performance characteristics
path = benchmark.create_benchmark(pathlib.Path("./montage"), cpu_work=100, data=10, percent_cpu=0.6)


# generate a Pegasus workflow
# translator = StreamFlowTranslator(benchmark.workflow)
# translator.translate(output_folder=pathlib.Path("/tmp/montage"))
# translator = ParslTranslator(benchmark.workflow)
# translator.translate(output_folder=pathlib.Path("./parsl"))
# print(benchmark.workflow.tasks)
#print(benchmark.workflow.tasks['mProject_00000001'].input_files)

#montage = Instance("./montage-benchmark-60.json")

#print(f"Files: {[i.file_id for i in montage.workflow.tasks['mProject_00000001'].files]}")
#print(f"Files:  {[i.file_id for i in benchmark.workflow.tasks['mProject_00000001'].input_files]}")
#print(benchmark.workflow.tasks['mProject_00000001'].task_id)
# print(f"Names:  {[montage.workflow.tasks for i in montage.workflow.tasks]}")



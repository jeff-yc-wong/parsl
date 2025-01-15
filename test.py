import parsl
import os
from pathlib import Path
import logging
from pprint import pprint
from parsl.app.app import python_app, bash_app
import parsl.executors
from parsl.monitoring.monitoring import MonitoringHub
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.addresses import address_by_hostname
from parsl.data_provider.files import File
from parsl.data_provider.data_manager import default_staging

parsl.clear()
# Emit log lines to the screen
# parsl.set_stream_logger(level=logging.DEBUG)

FILENAME = "debug.log"

local_htex = Config(
    executors=[
        HighThroughputExecutor(
            label="htex_Local",
            worker_debug=True,
            cores_per_worker=1,
            max_workers=1,
            provider=LocalProvider(
                channel=LocalChannel(),
                init_blocks=1,
                max_blocks=1,
            ),
        )
    ],
    strategy=None,
#     monitoring=MonitoringHub(
#        hub_address=address_by_hostname(),
#        monitoring_debug=False,
#        resource_monitoring_interval=10,
#    ),
)

parsl.clear()
#parsl.load(local_threads)
parsl.load(local_htex)

@bash_app
def generate(val, outputs=[], parsl_resource_specification={"cpu_core": 1}):
    return "echo $(( RANDOM )) &> {}".format(outputs[0])

@bash_app
def concat(inputs=[], outputs=[]):
    return "cat {0} > {1}".format(" ".join(i.filepath for i in inputs), outputs[0])

@python_app
def total(inputs=[]):
    total = 0
    with open(inputs[0], 'r') as f:
        for l in f:
            total += int(l)
    return total


@python_app
def barrier():
    return

# Create 5 files with semi-random numbers
output_files = []
for i in range (5):
     output_files.append(generate(i, outputs=[File(os.path.join(os.getcwd(), 'random-%s.txt' % i))]))

# Concatenate the files into a single file
cc = concat(inputs=[i.outputs[0] for i in output_files],
            outputs=[File(os.path.join(os.getcwd(), 'combined.txt'))])

# Calculate the sum of the random numbers
total = total(inputs=[cc.outputs[0]])


all_task = parsl.dfk().tasks

pprint(all_task)


# NOTE: build the workflow graph
task_graph = {}

filepath_to_id = {}

for i, val in all_task.items():
    task = {}

    task["func_name"] = val["func_name"]
    task["task_id"] = val["id"]
    print("Task Name:", val["func_name"])
    if "outputs" in val["kwargs"]:
        print("Outputs:")
        task["outputs"] = []
        for i in val["kwargs"]["outputs"]:
            print("  - ", i)
            task["outputs"].append(i.filepath)
            filepath_to_id[i.filepath] = val["id"]

    if "inputs" in val["kwargs"]:
        print("Inputs:")
        task["inputs"] = []
        for i in val["kwargs"]["inputs"]:
            print("  - ", i.filepath)
            task["inputs"].append(i.filepath)
            filepath_to_id[i.filepath] = val["id"]

    task_graph[val["id"]] = task

pprint(task_graph)
pprint(filepath_to_id)


# Task Info Dict
# {
#     "task_id": 3,
#     "func_name": "generate",
#     "childrens": 4,
#     "amount_of_data": 100, # in bytes
#     "computation": 10, # in seconds, for some maybe i can do percent_cpu * cpu_work
# }   


barrier()
print (total.result())

print(parsl.dfk().cleanup())

from sqlalchemy import create_engine, inspect
from parsl.monitoring.queries.pandas import *
import pandas as pd
import re
import ast
import argparse
from pathlib import Path
import json
from parsl import VERSION

def main():
    parser = argparse.ArgumentParser(description="Analyze a parsl workflow run.")
    parser.add_argument("--workflow", type=str, help="Path to the workflow run dir.", required=True)
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("-i", "--iteration", type=int, help="Iteration number for the groundtruth file", default=0)

    args = parser.parse_args()

    if args.workflow:
        workflow_path = Path(args.workflow)

        if not workflow_path.exists():
            print(f"Workflow path '{workflow_path}' does not exist.")
            return

        generate_groundtruth(workflow_path, iteration=args.iteration)

def generate_groundtruth(workflow_path: Path, iteration: int = 0):
    # Create a database connection (Replace with your DB details)
    sql_path = "sqlite:///" + str(workflow_path.absolute() / "runinfo" / "monitoring.db")

    engine = create_engine(sql_path)  # For SQLite

    # Create an inspector object
    inspector = inspect(engine)

    # Get list of table names
    tables = inspector.get_table_names()

    print("Tables in the database:", tables)

    df = pd.read_sql("SELECT * FROM workflow", engine)

    # assert len(df) == 1, "More than one workflow found."

    df['time_began'] = pd.to_datetime(df['time_began'])
    df['time_completed'] = pd.to_datetime(df['time_completed'])

    workflow_json_path = workflow_path / "jsons"
    with open(workflow_json_path / "workflow.json", "r") as f:
        workflow_json = json.load(f)

        tasks = workflow_json['workflow']['execution']['tasks']

        # TODO: can prob move to a function
        matches = {}
        reg_matches = []
        runinfo_path = workflow_path / "runinfo"

        folders = [str(folder.name) for folder in runinfo_path.iterdir() if folder.is_dir() and str(folder.name).isdigit()]

        sorted_runs = sorted(folders, key=int)

        for run in sorted_runs:
            workflow_id = df.iloc[-1]['run_id']

            with open(f'{workflow_path}/runinfo/{run}/parsl.log', 'rb') as f:
                # regex pattern for matching
                pattern = "^.*Run id is: (.*)$"
                # Read the file line by line
                for line in f.readlines():
                    # Matching for task done lines
                    match = re.match(pattern, line.decode('utf-8'))
                    if match:
                        workflow_id = match.group(1)
                        print(f"Workflow ID: {workflow_id}")
                        break

            tasks_df = tasks_for_workflow(workflow_id, engine)

            tries_df = tries_for_workflow(workflow_id, engine)

            tries_df['task_time_returned'] = pd.to_datetime(tries_df['task_time_returned'])
            tries_df['task_try_time_running'] = pd.to_datetime(tries_df['task_try_time_running'])
            tasks_df['task_time_invoked'] = pd.to_datetime(tasks_df['task_time_invoked'])

            tries_df['runtime'] = (tries_df['task_time_returned'] - tries_df['task_try_time_running']).dt.total_seconds()

            with open(f'{workflow_path}/runinfo/{run}/htex_docker/debug.log', 'rb') as f:
                # regex pattern for matching
                pattern = "^.*Task done: ({.*}).*$"
                reg_pattern = "^.*Registration info for manager b'.*': ({.*}).*$"

                # Read the file line by line
                for line in f.readlines():
                    # Matching for task done lines
                    match = re.match(pattern, line.decode('utf-8'))
                    if match:
                        info = ast.literal_eval(match.group(1))
                        matches[info['task']] = info

                    # Matching for worker registration
                    match = re.match(reg_pattern, line.decode('utf-8'))
                    if match:
                        info = ast.literal_eval(match.group(1))
                        reg_matches.append(info)

                if len(matches) != len(tasks):
                    print(f"Number of tasks ({len(tasks)}) and number of matches ({len(matches)}) do not match.")
                    print(f"Skipping run #{run}")
                    continue

            ############################################################################################################

            for task in tasks:

                task_id = task['id']

                row = tries_df[tries_df['task_func_name'] == task_id].iloc[0]

                task_start_time = pd.to_datetime(row['task_try_time_running']).isoformat()
                task_runtime = row['runtime']

                cpu_runtime = task['runtimeInSeconds']

                task['runtimeInSeconds'] = task_runtime
                task['executedAt'] = task_start_time
                task['machines'] = [matches[task_id]['worker']]
                task['avgCPU'] = round((cpu_runtime / task_runtime) * 100, 2)
                task['coreCount'] = 1

            workflow_makespan = (tries_df['task_time_returned'].max() - tries_df['task_try_time_running'].min()).total_seconds()

            workflow_json['workflow']['execution']['makespanInSeconds'] = workflow_makespan
            print("Total runtime in seconds:", workflow_makespan)

            machines = []
            for machine in reg_matches:
                machine_dict = {}

                machine_dict['nodeName'] = machine['hostname']
                machine_dict['system'] = machine['os'].lower()

                cpu_info = {
                    "coreCount": 1,
                    "speedInMHz":  machine['cpu_speed']
                }
                machine_dict['cpu'] = cpu_info

                machines.append(machine_dict)

            workflow_json['workflow']['execution']['machines'] = machines

            runtime_system = {
                "name": "parsl",
                "version": VERSION,
                "url": "https://github.com/jeff-yc-wong/parsl/tree/scheduling_using_simulation"
            }

            workflow_json['runtimeSystem'] = runtime_system

            with open(f"./groundtruth/groundtruth_{workflow_json['name']}_{int(run)}.json", "w") as f:
                json.dump(workflow_json, f, indent=4)



if __name__ == "__main__":
    main()

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
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--workflow", type=str, help="Path to the workflow run dir.")
    group.add_argument("--path", type=str, help="Path containing Parsl workflow directories.")
    group2 = parser.add_mutually_exclusive_group()
    group2.add_argument("--cpu-only", action="store_true", help="Analyze workflow with only CPU tasks.")
    group2.add_argument("--io-only", action="store_true", help="Analyze workflow with only IO tasks")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--outdir", default="./groundtruth", type=str, help="Output directory for groundtruth files.")

    args = parser.parse_args()

    output_dir = Path(args.outdir)

    if args.workflow:
        workflow_path = Path(args.workflow)

        if not workflow_path.exists():
            print(f"Workflow path '{workflow_path}' does not exist.")
            return

        generate_groundtruth(workflow_path, outdir=output_dir)

    if args.path:
        base_path = Path(args.path).resolve()

        for workflow_path in base_path.iterdir():
            if workflow_path.is_dir():
                if args.cpu_only and not workflow_path.name.endswith("_cpu"):
                    continue
                if args.io_only and not workflow_path.name.endswith("_io"):
                    continue
                print(f"Processing workflow {workflow_path}")
                generate_groundtruth(workflow_path, outdir=output_dir)

def generate_groundtruth(workflow_path: Path, outdir: Path):
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
        runinfo_path = workflow_path / "runinfo"

        folders = [str(folder.name) for folder in runinfo_path.iterdir() if folder.is_dir() and str(folder.name).isdigit()]

        sorted_runs = sorted(folders, key=int)

        for run in sorted_runs:
            print(f"Currently process run #{run}")
            workflow_id = df.iloc[-1]['run_id']

            matches = {}
            reg_matches = []

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


            workflow_makespan = (tries_df['task_time_returned'].max() - tries_df['task_try_time_running'].min()).total_seconds()

            workflow_json['workflow']['execution']['makespanInSeconds'] = workflow_makespan
            print("Total runtime in seconds:", workflow_makespan)



if __name__ == "__main__":
    main()

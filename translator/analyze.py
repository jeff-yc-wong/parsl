from sqlalchemy import create_engine, inspect
from parsl.monitoring.queries.pandas import *
import pandas as pd
import re
import ast

# Create a database connection (Replace with your DB details)
engine = create_engine("sqlite:///runinfo/monitoring.db")  # For SQLite

# Create an inspector object
inspector = inspect(engine)

# Get list of table names
tables = inspector.get_table_names()

print("Tables in the database:", tables)

df = pd.read_sql("SELECT * FROM workflow", engine)

workflow_id = df.iloc[-1]['run_id']

tasks_df = tasks_for_workflow(workflow_id, engine)

tries_df = tries_for_workflow(workflow_id, engine)

tries_df['task_time_returned'] = pd.to_datetime(tries_df['task_time_returned'])
tries_df['task_try_time_running'] = pd.to_datetime(tries_df['task_try_time_running'])
tasks_df['task_time_invoked'] = pd.to_datetime(tasks_df['task_time_invoked'])

tries_df['runtime'] = (tries_df['task_time_returned'] - tries_df['task_try_time_running']).dt.total_seconds()

print(tries_df[['task_func_name', 'task_try_time_running', 'task_time_returned', 'runtime']])

print("Total runtime in seconds:", (tries_df['task_time_returned'].max() - tasks_df['task_time_invoked'].min()).total_seconds())

nodes_df = nodes_for_workflow(workflow_id, engine)
# print(nodes_df[['id', 'run_id', 'block_id', 'worker_count']])


with open('runinfo/000/htex_docker/debug.log', 'rb') as f:

    pattern = "^.*Task done: ({.*}).*$"

    # Read the file line by line
    matches = []
    for line in f.readlines():
        match = re.match(pattern, line.decode('utf-8'))
        if match:
            matches.append(ast.literal_eval(match.group(1)))

    # print(matches)

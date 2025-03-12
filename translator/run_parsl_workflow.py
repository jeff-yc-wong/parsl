import argparse
import subprocess
from pathlib import Path

def process_workflow(directory, args):
    """Run the required commands for a given Parsl workflow directory."""
    try:
        workflow_dir = Path(directory).resolve()
        
        # Kill existing docker processes
        subprocess.run(["bash", "kill_docker.sh"], check=True, cwd=workflow_dir)
        
        # Start the workers
        subprocess.run(["bash", "start_workers.sh", str(args.num_workers)], check=True, cwd=workflow_dir)
        
        # Run the Parsl workflow
        cmd = ["python", "parsl_workflow.py", "--docker", "--num_workers", str(args.num_workers)]

        if args.verbose:
            cmd.append("--verbose")

        if args.task_selection_scheme:
            cmd.extend(["--task_selection_scheme", args.task_selection_scheme])

        if args.worker_selection_scheme:
            cmd.extend(["--worker_selection_scheme", args.worker_selection_scheme])

        if args.metric:
            cmd.extend(["--metric", args.metric])

        if args.calibration:
            cmd.extend(["--calibration", args.calibration])

        if args.num_threads:
            cmd.extend(["--num_threads", str(args.num_threads)])

        if args.simulate:
            cmd.append("--simulate")

        subprocess.run(cmd, check=True, cwd=workflow_dir)
        
        print(f"Successfully processed workflow in {workflow_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {workflow_dir}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Process Parsl workflow directories.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--path", type=str, help="Path containing Parsl workflow directories.")
    group.add_argument("--workflow", type=str, help="Path of a single workflow directory to process.")
    parser.add_argument("--num_workers", default=2, type=int, help="Number of workers to start.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity.")
    parser.add_argument("-tss", "--task_selection_scheme", default="fcfs", type=str, help="The task selection scheme to use.")
    parser.add_argument("-wss", "--worker_selection_scheme", default="random", type=str, help="The worker selection scheme to use.")
    parser.add_argument("-m", "--metric", default="makespan", type=str, help="The metric to optimize for.")
    parser.add_argument("-c", "--calibration", default=None, type=str, help="The calibration file to use.")
    parser.add_argument("-n", "--num_threads", default=1, type=int, help="The number of threads to use.")
    parser.add_argument("--simulate", action="store_true", help="Run the workflow in simulation mode.")
    parser.add_argument("--num_workers", default=2, type=int, help="The number of workers to use.")
    args = parser.parse_args()

    if args.path:
        base_path = Path(args.path).resolve()

        # Get all directories in the given path
        for entry in base_path.iterdir():
            if entry.is_dir():
                print(f"Processing workflow in {entry}...")
                process_workflow(entry, args)

    if args.workflow:
        process_workflow(args.workflow, args)

if __name__ == "__main__":
    main()

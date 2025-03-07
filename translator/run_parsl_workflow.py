import argparse
import subprocess
from pathlib import Path

def process_workflow(directory, num_workers):
    """Run the required commands for a given Parsl workflow directory."""
    try:
        workflow_dir = Path(directory).resolve()
        
        # Kill existing docker processes
        subprocess.run(["bash", "kill_docker.sh"], check=True, cwd=workflow_dir)
        
        # Start the workers
        subprocess.run(["bash", "start_workers.sh", str(num_workers)], check=True, cwd=workflow_dir)
        
        # Run the Parsl workflow
        subprocess.run(["python", "parsl_workflow.py", "--docker", "--num_workers", str(num_workers)], check=True, cwd=workflow_dir)
        
        print(f"Successfully processed workflow in {workflow_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing {workflow_dir}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Process Parsl workflow directories.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--path", type=str, help="Path containing Parsl workflow directories.")
    group.add_argument("--workflow", type=str, help="Path of a single workflow directory to process.")
    parser.add_argument("--num_workers", default=2, type=int, help="Number of workers to start.")
    args = parser.parse_args()

    if args.path:
        base_path = Path(args.path).resolve()

        # Get all directories in the given path
        for entry in base_path.iterdir():
            if entry.is_dir():
                print(f"Processing workflow in {entry}...")
                process_workflow(entry, args.num_workers)

    if args.workflow:
        process_workflow(args.workflow, args.num_workers)

if __name__ == "__main__":
    main()

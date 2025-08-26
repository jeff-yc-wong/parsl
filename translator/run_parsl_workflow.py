import sys
import argparse
import subprocess
import paramiko
import ipaddress
from pathlib import Path
from analyze import generate_groundtruth

# groundtruth_dir = Path("./groundtruth_25may1")
def parse_ip(ip_str):
    try:
        return ipaddress.ip_address(ip_str)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid IP address: {ip_str}")

def process_workflow(directory, args):
    """Run the required commands for a given Parsl workflow directory."""
    try:
        workflow_dir = Path(directory).resolve()
        
        # TODO: use paramiko to ssh into remote servers to kill/start docker workers
        if args.ip is not None:
            for ip in args.ip:
                hostname = str(ip)
                port = 22
                username = 'cc'
                key_filename = '/home/cc/.ssh/id_ed25519'

                try:
                    client = paramiko.SSHClient()
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    client.connect(hostname, port=port, username=username, key_filename=key_filename)
                        
                    # Kill existing docker processes
                    print(f"Killing existing docker processes on {hostname}...")
                    stdin, stdout, stderr = client.exec_command("bash kill_docker.sh")
                    # Print command output
                    for line in stdout:
                        print(line.strip())
                    for line in stderr:
                        print(line.strip())
                    
                    # Start the workers
                    print(f"Starting workers on {hostname} with {args.num_workers} workers...")
                    stdin, stdout, stderr = client.exec_command(f"bash start_workers.sh {args.num_workers}")
                    # Print command output
                    for line in stdout:
                        print(line.strip())
                    for line in stderr:
                        print(line.strip())

                except Exception as e:
                    print(f"Error connecting to {hostname}: {e}")
                    exit(1)
                finally:
                    client.close()
        else:
            # If no IPs are provided, assume local execution
            print("No IPs provided, running locally...")

            # Kill existing docker processes
            subprocess.run(["bash", "kill_docker.sh"], check=True, cwd=workflow_dir)
            
            # Start the workers
            subprocess.run(["bash", "start_workers.sh", str(args.num_workers)], check=True, cwd=workflow_dir)

        # Run the Parsl workflow
        cmd = ["python", "parsl_workflow.py", "--docker", "--num_workers", str(args.num_workers)]

        if args.verbose:
            cmd.append("--verbose")

        if args.ip:
            ip_list = [str(ip) for ip in args.ip]
            csv_ip = ",".join(ip_list)
            cmd.extend(["--ip", csv_ip])

        if not args.clean:
            if args.task_selection_scheme:
                cmd.extend(["--task_selection_scheme", args.task_selection_scheme])

            if args.worker_selection_scheme:
                cmd.extend(["--worker_selection_scheme", args.worker_selection_scheme])

            if args.simulate:
                cmd.extend(["--metric", args.metric])
                cmd.extend(["--calibration", args.calibration])
                cmd.extend(["--num_threads", str(args.num_threads)])
                cmd.append("--simulate")

                if workflow_dir.name.endswith("_io"):
                    sub_dir = "io_only"    
                elif workflow_dir.name.endswith("_cpu"):
                    sub_dir = "cpu_only"    
                else:
                    sub_dir = "cpu_io"
                
                # NOTE: in case we want send in groundtruth as workflow json?
                # json_name = f"groundtruth_{workflow_dir.name}_0.json"
                # workflow_file_path = groundtruth_dir / sub_dir / workflow_dir.name / json_name
                # if workflow_file_path.exists():
                #     cmd.extend(["--workflow_file", str(workflow_file_path.absolute())])
                # else:
                #     print(f"Error: Can't find corresponding workflow json file. Tried {workflow_file_path}")
                #     exit(-1)

                # TODO: change ["workflows"]["execution"]["machines"] {workflow_dir}/jsons/workflow.json to match number of workers
        try: 
            print(" ".join(cmd))
            subprocess.run(cmd, check=True, cwd=workflow_dir, stdout=sys.stdout, stderr=sys.stderr)
        except Exception as e:
            print(f"Error processing {workflow_dir}: {e}")
            exit(-1)


        # TODO: kill docker workers
        print(f"Successfully processed workflow in {workflow_dir}")

        # TODO: analyze the result and store it in the outdir
        generate_groundtruth(workflow_dir, args.outdir)
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
    parser.add_argument("-i", "--iterations", default=1, type=int, help="Number of iterations to run.")
    parser.add_argument("--ip", type=parse_ip, nargs='*', help="A space separated list of ip's to use for workers")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--io_only", action="store_true", help="Run workflow with only IO tasks.")
    group.add_argument("--cpu_only", action="store_true", help="Run workflow with only CPU tasks")
    group.add_argument("--all", action="store_true", help="Run all workflows in the given path.")
    parser.add_argument("--clean", action="store_true", help="flag for clean parsl workflows (i.e. workflows without scheudling info)")
    parser.add_argument("--outdir", type=str, default="./groundtruth",  help="Path to the generated wfformat json file (defaults to ./groundtruth")
    args = parser.parse_args()
    if args.path:
        base_path = Path(args.path).resolve()

        for i in range(args.iterations):
            # Get all directories in the given path
            for entry in base_path.iterdir():
                if entry.is_dir():
                    if args.io_only and not entry.name.endswith("_io"):
                        continue
                    if args.cpu_only and not entry.name.endswith("_cpu"):
                        continue
                    if not args.all and (entry.name.endswith("_io") or entry.name.endswith("_cpu")):
                        continue

                    print(f"Processing workflow in {entry}...")
                    process_workflow(entry, args)

            print(f"Iteration {i}: Successfully processed all workflows in {base_path}")

    if args.workflow:
        for _ in range(args.iterations):
            process_workflow(args.workflow, args)

if __name__ == "__main__":
    main()

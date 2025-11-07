import sys
import os
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

def validate_worker_and_core_speed_config(args):
    """Validate and normalize worker and core speed configuration based on IPs."""
    
    def validate_list_config(config_list, config_name, default_value=None):
        """Helper function to validate and normalize list configurations."""
        if config_list is None:
            return default_value
        
        if args.ip is None:
            # Local execution - use first value or default
            if isinstance(config_list, list):
                return config_list[0] if config_list else default_value
            return config_list
        
        # Remote execution with IPs
        num_ips = len(args.ip)
        
        if isinstance(config_list, list):
            if len(config_list) == 1:
                # Single value for all IPs
                return [config_list[0]] * num_ips
            elif len(config_list) == num_ips:
                # One value per IP
                return config_list
            else:
                raise argparse.ArgumentTypeError(
                    f"Number of {config_name} ({len(config_list)}) must be 1 or match number of IPs ({num_ips})"
                )
        else:
            # Single value for all IPs
            return [config_list] * num_ips
    
    # Validate worker configuration
    worker_config = validate_list_config(args.num_workers, "worker counts", 2)
    
    # Validate core speed configuration
    core_speed_config = validate_list_config(args.core_speed, "core speeds", None)
    
    return worker_config, core_speed_config

def process_workflow(directory, args):
    """Run the required commands for a given Parsl workflow directory."""
    try:
        workflow_dir = Path(directory).resolve()
        
        # Validate and get worker and core speed configuration
        worker_config, core_speed_config = validate_worker_and_core_speed_config(args)
        
        if args.ip is not None:
            for i, ip in enumerate(args.ip):
                hostname = str(ip)
                port = 22
                username = 'cc'
                key_filename = str(Path('~/.ssh/id_ed25519').expanduser())
                num_workers_for_ip = worker_config[i]

                try:
                    client = paramiko.SSHClient()
                    client.load_system_host_keys()
                    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    client.connect(hostname, port=port, username=username)
                        
                    # Kill existing docker processes
                    print(f"Killing existing docker processes on {hostname}...")
                    stdin, stdout, stderr = client.exec_command("cd ~/sus_env && bash kill_docker.sh")
                    # Print command output
                    for line in stdout:
                        print(line.strip())
                    for line in stderr:
                        print(line.strip())
                    
                    # Start the workers
                    print(f"Starting workers on {hostname} with {num_workers_for_ip} workers...")
                    start_index = sum(worker_config[:i])  # Calculate cumulative start index automatically
                    cmd = f"cd ~/sus_env && bash start_workers.sh {num_workers_for_ip} -s {start_index}"
                    if core_speed_config is not None:
                        core_speed_for_ip = core_speed_config[i]
                        cmd += f" --core_speed {core_speed_for_ip}"
                    stdin, stdout, stderr = client.exec_command(cmd)
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

            env_dir = Path.home().resolve() / "sus_env"

            # Kill existing docker processes
            subprocess.run(["bash", "kill_docker.sh"], check=True, cwd=env_dir)
            
            # Start the workers
            cmd = ["bash", "start_workers.sh", str(worker_config)]
            if core_speed_config is not None:
                cmd.extend(["--core_speed", str(core_speed_config)])
            subprocess.run(cmd, check=True, cwd=env_dir)

        # Run the Parsl workflow
        total_workers = sum(worker_config) if isinstance(worker_config, list) else worker_config
        cmd = ["python", "parsl_workflow.py", "--docker", "--num_workers"]

        num_workers_list = [str(num_workers) for num_workers in args.num_workers]
        cmd.extend(num_workers_list)

        if args.verbose:
            cmd.append("--verbose")

        if args.ip:
            ip_list = [str(ip) for ip in args.ip]
            cmd.extend(["--ip"])
            cmd.extend(ip_list)

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
    parser.add_argument("--num_workers", nargs='*', default=2, type=int, 
                        help="Number of workers to start. Can be a single integer (applied to all IPs) or a list of integers (one per IP).")
    parser.add_argument("-wd", "--worker-debug", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase output verbosity.")
    parser.add_argument("-tss", "--task_selection_scheme", default="fcfs", type=str, help="The task selection scheme to use.")
    parser.add_argument("-wss", "--worker_selection_scheme", default="random", type=str, help="The worker selection scheme to use.")
    parser.add_argument("-m", "--metric", default="makespan", type=str, help="The metric to optimize for.")
    parser.add_argument("-c", "--calibration", default=None, type=str, help="The calibration file to use.")
    parser.add_argument("-n", "--num_threads", default=1, type=int, help="The number of threads to use.")
    parser.add_argument("--simulate", action="store_true", help="Run the workflow in simulation mode.")
    parser.add_argument("-i", "--iterations", default=1, type=int, help="Number of iterations to run.")
    parser.add_argument("--ip", type=parse_ip, nargs='*', help="A space separated list of IP addresses to use for workers")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--io_only", action="store_true", help="Run workflow with only IO tasks.")
    group.add_argument("--cpu_only", action="store_true", help="Run workflow with only CPU tasks")
    group.add_argument("--all", action="store_true", help="Run all workflows in the given path.")
    parser.add_argument("--clean", action="store_true", help="flag for clean parsl workflows (i.e. workflows without scheudling info)")
    parser.add_argument("--outdir", type=str, default="./groundtruth",  help="Path to the generated wfformat json file (defaults to ./groundtruth")
    parser.add_argument("--core_speed", nargs='*', type=float, help="Core speed of workers in a machine (IP) as a float value. Can be a single float (applied to all IPs) or a list of floats (one per IP).")
    args = parser.parse_args()

    # Check if the 

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

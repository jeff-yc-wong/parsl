#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2024 The WfCommons Team.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

import pathlib
import argparse
import subprocess
import os
import platform
import psutil
from wfcommons.wfinstances import Instance
from wfcommons.common.machine import Machine, MachineSystem

def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument("--workflow", type=str, help="Path to the workflow JSON file.")
    parser.add_argument("--all", action="store_true", help="Create benchmarks for all workflows")
    parser.add_argument("--outdir",default=pathlib.Path.cwd().joinpath("cpu_benchmarks"),
                        help="Output directory in which to store the translated files")
    parser.add_argument("-r", "--run", action="store_true",)

    return parser


def start_bench(wf_input:str, outdir_path: pathlib.Path, run: bool):
    try:
        instance = Instance(wf_input)
    except Exception as e:
        raise e

    workflow_obj = instance.workflow

    for task in workflow_obj.tasks.values():
        cmd = []

        for arg in task.args:
            if not (arg.startswith("--input-files") or arg.startswith("--output-files")):
                cmd.append(arg)

        cmd.insert(0, task.program)
        cmd.insert(0, "time")

        cmd = ["taskset", "-c", "0", "bash", "-c", " ".join(cmd)]

        print(cmd)

        cpu_info = {
            "coreCount": os.cpu_count(),
            
            "vendor": platform.processor()
        }

        if psutil.cpu_freq().max:
            cpu_info["speedInMHz"] =  int(psutil.cpu_freq().max)
        # Get system information
        system_info = Machine(
                name=platform.node(),
                cpu = cpu_info,
                system= MachineSystem('macos') if platform.system() == "Darwin" else MachineSystem(platform.system().lower()),
                architecture=platform.machine(),
                release=platform.release(),
                memory=psutil.virtual_memory().total
            )

        if run:

            # Create a copy of the current environment and modify it
            env = os.environ.copy()
            env["TIMEFORMAT"] = "%3R"

            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, env=env)

            elapsed_time = result.stderr.decode('utf-8').splitlines()[-1]
            print(elapsed_time)

            task.runtime = float(elapsed_time)

        task.machines = [system_info]

    workflow_obj.write_json(outdir_path.joinpath(f"{workflow_obj.name}.json"))

def main():
    parser = get_parser()
    args = parser.parse_args()

    outdir_path = args.outdir

    if not isinstance(outdir_path, pathlib.Path):
        outdir_path = pathlib.Path(outdir_path)

    outdir_path.mkdir(parents=True, exist_ok=True)

    if args.workflow:
        start_bench(args.workflow, outdir_path, args.run)
    elif args.all:
        all_path = pathlib.Path("./benchmarks")

        for wf in all_path.iterdir():
            if wf.is_dir():
                for wf_file in wf.iterdir():
                    if wf_file.name.endswith(".json"):
                        start_bench(wf_file, outdir_path, args.run)
    return 0


if __name__ == "__main__":
    main()

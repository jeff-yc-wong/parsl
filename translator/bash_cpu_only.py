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
from wfcommons.wfinstances import Instance

this_dir = pathlib.Path(__file__).resolve().parent

def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "wfformat_file", help="path to WfFormat JSON input file")
    parser.add_argument("--outdir",default=pathlib.Path.cwd().joinpath("bash_runs"),
                        help="Output directory in which to store the translated files")
    parser.add_argument("-r", "--run", action="store_true",)

    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    wf_input = args.wfformat_file
    outdir_path = args.outdir

    if not isinstance(outdir_path, pathlib.Path):
        outdir_path = pathlib.Path(outdir_path)

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

        if args.run:

            # Create a copy of the current environment and modify it
            env = os.environ.copy()
            env["TIMEFORMAT"] = "%3R"

            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, env=env)


            #print(result.stdout.decode('utf-8'))
            elapsed_time = result.stderr.decode('utf-8').splitlines()[-1]

            task.runtime = float(elapsed_time)

    workflow_obj.write_json(outdir_path.joinpath("workflow.json"))

    return 0


if __name__ == "__main__":
    main()

